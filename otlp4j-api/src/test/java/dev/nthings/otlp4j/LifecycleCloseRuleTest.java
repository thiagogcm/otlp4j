package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import dev.nthings.otlp4j.pipeline.Lifecycle;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Pins the uniform [Lifecycle#close] rule: a best-effort drain that swallows a failure or
/// cancellation as a WARN instead of throwing, so it is safe in try-with-resources.
@DisplayName("Lifecycle.close() swallow-and-warn rule")
class LifecycleCloseRuleTest {

    @DisplayName("close() swallows a failing shutdown drain instead of throwing")
    @Test
    void closeSwallowsFailingDrain() {
        var drained = new AtomicBoolean();
        Lifecycle failing = timeout -> {
            drained.set(true);
            return CompletableFuture.failedFuture(new IllegalStateException("drain blew up"));
        };
        assertThatCode(failing::close).doesNotThrowAnyException();
        assertThat(drained).isTrue();
    }

    @DisplayName("close() swallows a cancelled shutdown drain instead of throwing")
    @Test
    void closeSwallowsCancelledDrain() {
        Lifecycle cancelled = timeout -> {
            var f = new CompletableFuture<Void>();
            f.cancel(true);
            return f;
        };
        assertThatCode(cancelled::close).doesNotThrowAnyException();
    }

    @DisplayName("close() drains cleanly within the default grace period on the happy path")
    @Test
    void closeDrainsCleanly() {
        var timeoutSeen = new java.util.concurrent.atomic.AtomicReference<Duration>();
        Lifecycle clean = timeout -> {
            timeoutSeen.set(timeout);
            return CompletableFuture.completedFuture(null);
        };
        assertThatCode(clean::close).doesNotThrowAnyException();
        assertThat(timeoutSeen.get()).isEqualTo(Lifecycle.DEFAULT_GRACE_PERIOD);
    }
}
