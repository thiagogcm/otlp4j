package dev.nthings.otlp4j.core.result;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.result.DeliveryResult.Accepted;
import dev.nthings.otlp4j.core.result.DeliveryResult.Partial;
import dev.nthings.otlp4j.core.result.DeliveryResult.Rejected;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for the [DeliveryResult] taxonomy: the three variants, every static factory, the
/// record invariants, the `permanentFrom` normalization, and the `fanOutMerge` port that preserves
/// the merged provenance and downstream marker the legacy merge dropped.
class DeliveryResultTest {

    private static final Provenance TRACES_EXPORTER =
            new Provenance(new ComponentId("otlp", ""), ComponentKind.EXPORTER, SignalDescriptor.TRACES);

    private static final Provenance METRICS_EXPORTER =
            new Provenance(new ComponentId("otlp", "2"), ComponentKind.EXPORTER, SignalDescriptor.METRICS);

    @Test
    @DisplayName("accepted carries provenance and acceptedStage completes with it")
    void acceptedFactories() throws Exception {
        var accepted = DeliveryResult.accepted(TRACES_EXPORTER);
        assertThat(accepted).isInstanceOf(Accepted.class);
        assertThat(accepted.provenance()).isEqualTo(TRACES_EXPORTER);
        assertThat(accepted).isEqualTo(new Accepted(TRACES_EXPORTER));
        assertThat(accepted).hasToString(new Accepted(TRACES_EXPORTER).toString());

        var stage = DeliveryResult.acceptedStage(TRACES_EXPORTER);
        assertThat(stage.toCompletableFuture().get()).isEqualTo(accepted);
    }

    @Test
    @DisplayName("partial requires a strictly positive rejection count and normalizes a null message")
    void partialFactory() {
        var partial = (Partial) DeliveryResult.partial(3, "dropped", TRACES_EXPORTER);
        assertThat(partial.rejectedItems()).isEqualTo(3);
        assertThat(partial.message()).isEqualTo("dropped");
        assertThat(partial.provenance()).isEqualTo(TRACES_EXPORTER);

        assertThat(new Partial(1, null, TRACES_EXPORTER).message()).isEmpty();
        assertThatThrownBy(() -> DeliveryResult.partial(0, "x", TRACES_EXPORTER))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("retryable and permanent factories set the disposition and LOCAL_FAILURE reason")
    void retryableAndPermanent() {
        var retryable = (Rejected) DeliveryResult.retryable("busy", TRACES_EXPORTER);
        assertThat(retryable.disposition()).isEqualTo(Disposition.RETRYABLE);
        assertThat(retryable.reason()).isEqualTo(DeliveryReason.LOCAL_FAILURE);
        assertThat(retryable.downstream()).isFalse();
        assertThat(retryable.cause()).isNull();

        var cause = new IllegalStateException("boom");
        var retryableWithCause = (Rejected) DeliveryResult.retryable("busy", cause, TRACES_EXPORTER);
        assertThat(retryableWithCause.cause()).isSameAs(cause);

        var permanent = (Rejected) DeliveryResult.permanent("bad", TRACES_EXPORTER);
        assertThat(permanent.disposition()).isEqualTo(Disposition.PERMANENT);
        assertThat(permanent.reason()).isEqualTo(DeliveryReason.LOCAL_FAILURE);

        var permanentWithCause = (Rejected) DeliveryResult.permanent("bad", cause, TRACES_EXPORTER);
        assertThat(permanentWithCause.cause()).isSameAs(cause);
    }

    @Test
    @DisplayName("throttled sets a retryAfter hint on a retryable rejection")
    void throttled() {
        var hint = Duration.ofSeconds(2);
        var throttled = (Rejected) DeliveryResult.throttled(hint, "slow down", TRACES_EXPORTER);
        assertThat(throttled.disposition()).isEqualTo(Disposition.RETRYABLE);
        assertThat(throttled.retryAfter()).isEqualTo(hint);

        var cause = new RuntimeException("q full");
        var throttledWithCause =
                (Rejected) DeliveryResult.throttled(hint, "slow down", cause, TRACES_EXPORTER);
        assertThat(throttledWithCause.cause()).isSameAs(cause);
    }

    @Test
    @DisplayName("admissionRefused, timedOut and shuttingDown set their reason codes as retryable")
    void reasonCodedRejections() {
        var hint = Duration.ofMillis(500);
        var admission = (Rejected) DeliveryResult.admissionRefused("at capacity", hint, TRACES_EXPORTER);
        assertThat(admission.disposition()).isEqualTo(Disposition.RETRYABLE);
        assertThat(admission.reason()).isEqualTo(DeliveryReason.ADMISSION_REFUSED);
        assertThat(admission.retryAfter()).isEqualTo(hint);

        var admissionNoHint = (Rejected) DeliveryResult.admissionRefused("at capacity", null, TRACES_EXPORTER);
        assertThat(admissionNoHint.retryAfter()).isNull();

        var timeout = (Rejected) DeliveryResult.timedOut("too slow", TRACES_EXPORTER);
        assertThat(timeout.reason()).isEqualTo(DeliveryReason.TIMEOUT);
        assertThat(timeout.disposition()).isEqualTo(Disposition.RETRYABLE);

        var shutdown = (Rejected) DeliveryResult.shuttingDown("draining", TRACES_EXPORTER);
        assertThat(shutdown.reason()).isEqualTo(DeliveryReason.SHUTDOWN);
        assertThat(shutdown.disposition()).isEqualTo(Disposition.RETRYABLE);
    }

    @Test
    @DisplayName("downstream sets the downstream flag with the caller's disposition and reason")
    void downstream() {
        var permanentDownstream = (Rejected) DeliveryResult.downstream(
                Disposition.PERMANENT, DeliveryReason.PROTOCOL_TRANSLATION, "bad response", null, null,
                TRACES_EXPORTER);
        assertThat(permanentDownstream.downstream()).isTrue();
        assertThat(permanentDownstream.disposition()).isEqualTo(Disposition.PERMANENT);
        assertThat(permanentDownstream.reason()).isEqualTo(DeliveryReason.PROTOCOL_TRANSLATION);

        var retryableDownstream = (Rejected) DeliveryResult.downstream(
                Disposition.RETRYABLE, DeliveryReason.LOCAL_FAILURE, "unavailable", null, Duration.ofSeconds(1),
                TRACES_EXPORTER);
        assertThat(retryableDownstream.downstream()).isTrue();
        assertThat(retryableDownstream.retryAfter()).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    @DisplayName("Rejected rejects a retryAfter on a permanent disposition or a non-positive hint")
    void rejectedInvariants() {
        assertThatThrownBy(() -> new Rejected(
                        Disposition.PERMANENT, DeliveryReason.LOCAL_FAILURE, "x", null, Duration.ofSeconds(1),
                        false, TRACES_EXPORTER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryable");
        assertThatThrownBy(() -> new Rejected(
                        Disposition.RETRYABLE, DeliveryReason.LOCAL_FAILURE, "x", null, Duration.ZERO, false,
                        TRACES_EXPORTER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("> 0");
        assertThatThrownBy(() -> new Rejected(
                        null, DeliveryReason.LOCAL_FAILURE, "x", null, null, false, TRACES_EXPORTER))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Rejected(
                        Disposition.RETRYABLE, null, "x", null, null, false, TRACES_EXPORTER))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(new Rejected(Disposition.RETRYABLE, DeliveryReason.LOCAL_FAILURE, null, null, null, false,
                        TRACES_EXPORTER).message())
                .isEmpty();
    }

    @Test
    @DisplayName("every variant requires a non-null provenance")
    void provenanceRequired() {
        assertThatThrownBy(() -> new Accepted(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Partial(1, "x", null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Rejected(
                        Disposition.RETRYABLE, DeliveryReason.LOCAL_FAILURE, "x", null, null, false, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("permanentFrom unwraps a CompletionException and normalizes the message")
    void permanentFromUnwraps() {
        var root = new IllegalStateException("root");
        var wrapped = new CompletionException(root);
        var result = (Rejected) DeliveryResult.permanentFrom("export", wrapped, TRACES_EXPORTER);
        assertThat(result.disposition()).isEqualTo(Disposition.PERMANENT);
        assertThat(result.reason()).isEqualTo(DeliveryReason.LOCAL_FAILURE);
        assertThat(result.cause()).isSameAs(root);
        assertThat(result.message()).isEqualTo("export: " + root);

        var direct = (Rejected) DeliveryResult.permanentFrom("export", root, TRACES_EXPORTER);
        assertThat(direct.cause()).isSameAs(root);
    }

    @Test
    @DisplayName("permanentFrom rethrows an Error and restores the interrupt flag")
    void permanentFromErrorAndInterrupt() {
        var error = new StackOverflowError();
        assertThatThrownBy(() -> DeliveryResult.permanentFrom("x", error, TRACES_EXPORTER))
                .isSameAs(error);

        try {
            var result = (Rejected) DeliveryResult.permanentFrom("x", new InterruptedException("stop"),
                    TRACES_EXPORTER);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(result.disposition()).isEqualTo(Disposition.PERMANENT);
        } finally {
            // Clear the interrupt so it does not leak into other tests on this thread.
            Thread.interrupted();
        }
    }

    @Test
    @DisplayName("fanOutMerge on an empty list is rejected")
    void fanOutMergeEmpty() {
        assertThatThrownBy(() -> DeliveryResult.fanOutMerge(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("fanOutMerge of all-accepted peers is accepted, preserving the first peer's provenance")
    void fanOutMergeAllAccepted() {
        var merged = DeliveryResult.fanOutMerge(List.of(
                DeliveryResult.accepted(TRACES_EXPORTER), DeliveryResult.accepted(METRICS_EXPORTER)));
        assertThat(merged).isInstanceOf(Accepted.class);
        assertThat(merged.provenance()).isEqualTo(TRACES_EXPORTER);
    }

    @Test
    @DisplayName("fanOutMerge reports the worst-case partial count and concatenates messages")
    void fanOutMergePartials() {
        var merged = (Partial) DeliveryResult.fanOutMerge(List.of(
                DeliveryResult.accepted(TRACES_EXPORTER),
                DeliveryResult.partial(2, "a", TRACES_EXPORTER),
                DeliveryResult.partial(5, "b", METRICS_EXPORTER)));
        assertThat(merged.rejectedItems()).isEqualTo(5);
        assertThat(merged.message()).isEqualTo("a; b");
        assertThat(merged.provenance()).isEqualTo(TRACES_EXPORTER);
    }

    @Test
    @DisplayName("a retryable fanOut merge carries the longest retryAfter and preserves downstream")
    void fanOutMergeRetryable() {
        var shortHint = DeliveryResult.throttled(Duration.ofSeconds(1), "one", TRACES_EXPORTER);
        var longHint = DeliveryResult.downstream(
                Disposition.RETRYABLE, DeliveryReason.LOCAL_FAILURE, "two", null, Duration.ofSeconds(3),
                METRICS_EXPORTER);
        var merged = (Rejected) DeliveryResult.fanOutMerge(List.of(shortHint, longHint));
        assertThat(merged.disposition()).isEqualTo(Disposition.RETRYABLE);
        assertThat(merged.retryAfter()).isEqualTo(Duration.ofSeconds(3));
        // The first rejecting peer's provenance and downstream flag win.
        assertThat(merged.provenance()).isEqualTo(TRACES_EXPORTER);
        assertThat(merged.downstream()).isFalse();
        assertThat(merged.message()).isEqualTo("one; two");
    }

    @Test
    @DisplayName("a permanent peer makes the merge permanent, drops the hint, and is attributed to that peer")
    void fanOutMergePermanentDominates() {
        var cause = new RuntimeException("perm cause");
        var retryable = DeliveryResult.throttled(Duration.ofSeconds(5), "retry", TRACES_EXPORTER);
        var permanent = (Rejected) DeliveryResult.permanent("nope", cause, METRICS_EXPORTER);
        var merged = (Rejected) DeliveryResult.fanOutMerge(List.of(retryable, permanent));
        assertThat(merged.disposition()).isEqualTo(Disposition.PERMANENT);
        assertThat(merged.retryAfter()).isNull();
        assertThat(merged.cause()).isSameAs(cause);
        // The permanent peer forced the disposition, so its provenance, reason, and cause are the
        // ones carried, even though a retryable peer came first in the list.
        assertThat(merged.provenance()).isEqualTo(METRICS_EXPORTER);
        assertThat(merged.reason()).isEqualTo(permanent.reason());
        assertThat(merged.downstream()).isEqualTo(permanent.downstream());
    }

    @Test
    @DisplayName("a later permanent downstream peer supplies the merged downstream marker and provenance")
    void fanOutMergePermanentDownstreamNotFirst() {
        // A retryable, non-downstream peer comes first; a permanent, downstream peer comes second.
        var retryable = DeliveryResult.retryable("busy", TRACES_EXPORTER);
        var permanentDownstream = (Rejected) DeliveryResult.downstream(
                Disposition.PERMANENT, DeliveryReason.PROTOCOL_TRANSLATION, "content rejected", null, null,
                METRICS_EXPORTER);
        var merged = (Rejected) DeliveryResult.fanOutMerge(List.of(retryable, permanentDownstream));
        assertThat(merged.disposition()).isEqualTo(Disposition.PERMANENT);
        // The permanent peer forced the outcome, so its downstream marker, reason, and provenance win
        // rather than the earlier retryable peer's.
        assertThat(merged.downstream()).isTrue();
        assertThat(merged.reason()).isEqualTo(DeliveryReason.PROTOCOL_TRANSLATION);
        assertThat(merged.provenance()).isEqualTo(METRICS_EXPORTER);
    }

    @Test
    @DisplayName("fanOutMerge of a single accepted peer with no rejections and no partials is accepted")
    void fanOutMergeSingleAccepted() {
        var merged = DeliveryResult.fanOutMerge(List.of(DeliveryResult.accepted(METRICS_EXPORTER)));
        assertThat(merged).isInstanceOf(Accepted.class);
        assertThat(merged.provenance()).isEqualTo(METRICS_EXPORTER);
    }
}
