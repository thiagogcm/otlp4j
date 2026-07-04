package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.Lifecycle;
import dev.nthings.otlp4j.pipeline.SharedLifecycle;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.pipeline.ops.Connector;
import dev.nthings.otlp4j.pipeline.ops.FailurePolicy;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Exercises the general [Connector] SPI through a custom signal-changing connector
/// (traces in, logs out): derive semantics, the failure-policy matrix, and lifecycle cascade.
@DisplayName("Connector SPI")
class ConnectorSpiTest {

    /// Emits one log record per non-empty trace batch; a custom cross-signal connector.
    private static final class SpanToLogConnector extends Connector<TracesData, LogsData> {

        SpanToLogConnector(Sink<? super LogsData> downstream, FailurePolicy policy) {
            super("span-to-log", downstream, policy);
        }

        @Override
        protected @Nullable LogsData derive(TracesData batch) {
            if (batch.spanCount() == 0) {
                return null;
            }
            return Fixtures.logsData(Fixtures.logRecord("spans=" + batch.spanCount(), LogRecord.Severity.INFO));
        }
    }

    /// A connector whose derive always throws, for the policy matrix.
    private static final class ThrowingConnector extends Connector<TracesData, LogsData> {

        ThrowingConnector(Sink<? super LogsData> downstream, FailurePolicy policy) {
            super("throwing", downstream, policy);
        }

        @Override
        protected LogsData derive(TracesData batch) {
            throw new IllegalStateException("derive boom");
        }
    }

    private static TracesData traces() {
        return Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));
    }

    private static ConsumeResult consume(Connector<TracesData, LogsData> connector) {
        return connector.consume(traces()).toCompletableFuture().join();
    }

    @DisplayName("derive output reaches the downstream and the input is accepted")
    @Test
    void deriveEmitsDownstream() {
        var seen = new AtomicReference<LogsData>();
        var connector = new SpanToLogConnector(logs -> {
            seen.set(logs);
            return ConsumeResult.acceptedStage();
        }, FailurePolicy.FAIL);

        assertThat(consume(connector)).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(seen.get()).isNotNull();
        assertThat(seen.get().logRecordCount()).isEqualTo(1);
    }

    @DisplayName("derive returning null emits nothing and accepts the input")
    @Test
    void deriveNullEmitsNothing() {
        var delivered = new AtomicInteger();
        var connector = new SpanToLogConnector(logs -> {
            delivered.incrementAndGet();
            return ConsumeResult.acceptedStage();
        }, FailurePolicy.FAIL);

        var result = connector.consume(new TracesData(java.util.List.of())).toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(delivered.get()).isZero();
    }

    @DisplayName("a derive throw is governed by the policy: BEST_EFFORT accepts, FAIL rejects")
    @Test
    void deriveThrowFollowsPolicy() {
        Sink<LogsData> downstream = logs -> ConsumeResult.acceptedStage();

        var bestEffort = consume(new ThrowingConnector(downstream, FailurePolicy.BEST_EFFORT));
        assertThat(bestEffort).isInstanceOf(ConsumeResult.Accepted.class);

        var fail = consume(new ThrowingConnector(downstream, FailurePolicy.FAIL));
        assertThat(fail).isInstanceOfSatisfying(ConsumeResult.Rejected.class, r -> {
            assertThat(r.retryable()).isFalse();
            assertThat(r.message()).contains("derive boom");
        });
    }

    @DisplayName("downstream Partial: BEST_EFFORT accepts, FAIL propagates retryably")
    @Test
    void downstreamPartialFollowsPolicy() {
        Sink<LogsData> partial = logs ->
                CompletableFuture.completedFuture(ConsumeResult.partial(1L, "1 record bad"));

        assertThat(consume(new SpanToLogConnector(partial, FailurePolicy.BEST_EFFORT)))
                .isInstanceOf(ConsumeResult.Accepted.class);

        var fail = consume(new SpanToLogConnector(partial, FailurePolicy.FAIL));
        assertThat(fail).isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                r -> assertThat(r.retryable()).isTrue());
    }

    @DisplayName("FAIL forwards the downstream retry intent, cause, and retry-after hint")
    @Test
    void failForwardsRetryIntentAndHint() {
        var cause = new IllegalStateException("backend full");
        Sink<LogsData> throttled = logs -> CompletableFuture.completedFuture(
                ConsumeResult.throttled(Duration.ofSeconds(3), "at capacity", cause));

        var forwarded = consume(new SpanToLogConnector(throttled, FailurePolicy.FAIL));
        assertThat(forwarded).isInstanceOfSatisfying(ConsumeResult.Rejected.class, r -> {
            assertThat(r.retryable()).isTrue();
            assertThat(r.retryAfter()).isEqualTo(Duration.ofSeconds(3));
            assertThat(r.cause()).isSameAs(cause);
        });

        Sink<LogsData> permanent = logs -> CompletableFuture.completedFuture(
                ConsumeResult.permanent("bad schema", cause));
        var forwardedPermanent = consume(new SpanToLogConnector(permanent, FailurePolicy.FAIL));
        assertThat(forwardedPermanent).isInstanceOfSatisfying(ConsumeResult.Rejected.class, r -> {
            assertThat(r.retryable()).isFalse();
            assertThat(r.cause()).isSameAs(cause);
        });
    }

    @DisplayName("a downstream throw, failed stage, or null stage rejects permanently under FAIL")
    @Test
    void downstreamFailureModesRejectUnderFail() {
        Sink<LogsData> throwing = logs -> { throw new IllegalStateException("sync boom"); };
        assertThat(consume(new SpanToLogConnector(throwing, FailurePolicy.FAIL)))
                .isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                        r -> assertThat(r.message()).contains("sync boom"));

        Sink<LogsData> failing = logs -> CompletableFuture.failedFuture(new IllegalStateException("async boom"));
        assertThat(consume(new SpanToLogConnector(failing, FailurePolicy.FAIL)))
                .isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                        r -> assertThat(r.message()).contains("async boom"));

        Sink<LogsData> nullStage = logs -> null;
        assertThat(consume(new SpanToLogConnector(nullStage, FailurePolicy.FAIL)))
                .isInstanceOfSatisfying(ConsumeResult.Rejected.class,
                        r -> assertThat(r.message()).contains("null stage"));
    }

    @DisplayName("BEST_EFFORT accepts the input through every downstream failure mode")
    @Test
    void bestEffortAcceptsThroughFailures() {
        Sink<LogsData> throwing = logs -> { throw new IllegalStateException("sync boom"); };
        Sink<LogsData> failing = logs -> CompletableFuture.failedFuture(new IllegalStateException("async boom"));
        Sink<LogsData> rejecting = logs -> CompletableFuture.completedFuture(ConsumeResult.permanent("no"));

        assertThat(consume(new SpanToLogConnector(throwing, FailurePolicy.BEST_EFFORT)))
                .isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(consume(new SpanToLogConnector(failing, FailurePolicy.BEST_EFFORT)))
                .isInstanceOf(ConsumeResult.Accepted.class);
        assertThat(consume(new SpanToLogConnector(rejecting, FailurePolicy.BEST_EFFORT)))
                .isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("an Error from the downstream propagates instead of following the policy")
    @Test
    void downstreamErrorPropagates() {
        Sink<LogsData> erroring = logs -> { throw new AssertionError("must escape"); };
        var connector = new SpanToLogConnector(erroring, FailurePolicy.BEST_EFFORT);
        assertThatThrownBy(() -> consume(connector)).isInstanceOf(AssertionError.class);
    }

    @DisplayName("retain() cascades to a SharedLifecycle downstream")
    @Test
    void retainCascades() {
        var retains = new AtomicInteger();

        class SharedDownstream implements Sink<LogsData>, SharedLifecycle {
            @Override public CompletionStage<ConsumeResult> consume(LogsData batch) {
                return ConsumeResult.acceptedStage();
            }
            @Override public void retain() {
                retains.incrementAndGet();
            }
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                return CompletableFuture.completedFuture(null);
            }
        }

        var connector = new SpanToLogConnector(new SharedDownstream(), FailurePolicy.BEST_EFFORT);
        connector.retain();
        assertThat(retains.get()).isEqualTo(1);
    }

    @DisplayName("shutdown runs the onShutdown hook before cascading to the downstream")
    @Test
    void shutdownHookRunsBeforeDownstreamCascade() {
        var order = new ArrayList<String>();

        class LifecycleDownstream implements Sink<LogsData>, Lifecycle {
            @Override public CompletionStage<ConsumeResult> consume(LogsData batch) {
                return ConsumeResult.acceptedStage();
            }
            @Override public CompletionStage<Void> shutdown(Duration timeout) {
                order.add("downstream");
                return CompletableFuture.completedFuture(null);
            }
            @Override public CompletionStage<Void> forceFlush(Duration timeout) {
                order.add("downstream-flush");
                return CompletableFuture.completedFuture(null);
            }
        }

        class BufferingConnector extends Connector<TracesData, LogsData> {
            BufferingConnector(Sink<? super LogsData> downstream) {
                super("buffering", downstream);
            }
            @Override protected @Nullable LogsData derive(TracesData batch) {
                return null;
            }
            @Override protected CompletionStage<Void> onShutdown(Duration timeout) {
                order.add("hook");
                return CompletableFuture.completedFuture(null);
            }
            @Override protected CompletionStage<Void> onForceFlush(Duration timeout) {
                order.add("hook-flush");
                return CompletableFuture.completedFuture(null);
            }
        }

        var connector = new BufferingConnector(new LifecycleDownstream());
        connector.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        connector.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
        assertThat(order).containsExactly("hook", "downstream", "hook-flush", "downstream-flush");
    }

    @DisplayName("shutdown and forceFlush complete when the downstream has no lifecycle")
    @Test
    void lifecycleNoopsWithoutDownstreamLifecycle() {
        var connector = new SpanToLogConnector(logs -> ConsumeResult.acceptedStage(), FailurePolicy.BEST_EFFORT);
        connector.shutdown(Duration.ofSeconds(1)).toCompletableFuture().join();
        connector.forceFlush(Duration.ofSeconds(1)).toCompletableFuture().join();
        connector.retain(); // no SharedLifecycle downstream: a no-op, must not throw
    }

    @DisplayName("an InterruptedException downstream failure restores the interrupt flag")
    @Test
    void interruptedDownstreamRestoresFlag() {
        Sink<LogsData> interrupted = logs -> CompletableFuture.failedFuture(new InterruptedException("stop"));
        var connector = new SpanToLogConnector(interrupted, FailurePolicy.FAIL);
        try {
            var result = consume(connector);
            assertThat(Thread.interrupted()).isTrue();
            assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
        } finally {
            Thread.interrupted();
        }
    }
}
