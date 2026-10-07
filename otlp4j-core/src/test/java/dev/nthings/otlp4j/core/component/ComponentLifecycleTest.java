package dev.nthings.otlp4j.core.component;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.result.FlushReport;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for the [Component] default methods: the no-op `flush`, and `close` shutting down
/// within the grace period while rethrowing a drain failure wrapped in an [IllegalStateException].
class ComponentLifecycleTest {

    /// A minimal component whose shutdown stage is programmable, so both the clean-drain and the
    /// failed-drain close paths can be exercised.
    private static final class FakeComponent implements Component {

        private final CompletionStage<Void> shutdownStage;
        private final AtomicBoolean shutdownCalled = new AtomicBoolean();

        FakeComponent(CompletionStage<Void> shutdownStage) {
            this.shutdownStage = shutdownStage;
        }

        @Override
        public CompletionStage<Void> start(ComponentContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext context) {
            shutdownCalled.set(true);
            return shutdownStage;
        }
    }

    @Test
    @DisplayName("flush defaults to a completed empty report")
    void flushDefault() {
        var component = new FakeComponent(CompletableFuture.completedFuture(null));
        var flush = component.flush(new FlushContext(Component.DEFAULT_GRACE_PERIOD));
        assertThat(flush.toCompletableFuture()).succeedsWithin(Duration.ofSeconds(1))
                .isEqualTo(FlushReport.empty());
    }

    @Test
    @DisplayName("close drains cleanly through shutdown when the stage completes")
    void closeCleanDrain() {
        var component = new FakeComponent(CompletableFuture.completedFuture(null));
        assertThatCode(component::close).doesNotThrowAnyException();
        assertThat(component.shutdownCalled).isTrue();
    }

    @Test
    @DisplayName("close rethrows a drain failure wrapped in an IllegalStateException")
    void closeRethrowsFailure() {
        var cause = new IllegalStateException("drain boom");
        var component = new FakeComponent(CompletableFuture.failedFuture(cause));
        assertThatThrownBy(component::close)
                .isInstanceOf(IllegalStateException.class)
                .hasRootCause(cause);
        assertThat(component.shutdownCalled).isTrue();
    }
}
