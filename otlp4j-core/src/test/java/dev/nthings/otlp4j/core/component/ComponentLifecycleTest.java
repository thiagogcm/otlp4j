package dev.nthings.otlp4j.core.component;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for the [Component] default methods: the no-op `forceFlush`, and `close` shutting down
/// within the grace period while swallowing a drain failure as a warning.
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
    @DisplayName("forceFlush defaults to a completed no-op")
    void forceFlushDefault() {
        var component = new FakeComponent(CompletableFuture.completedFuture(null));
        var flush = component.forceFlush(new FlushContext(Component.DEFAULT_GRACE_PERIOD));
        assertThat(flush.toCompletableFuture()).isCompleted();
    }

    @Test
    @DisplayName("close drains cleanly through shutdown when the stage completes")
    void closeCleanDrain() {
        var component = new FakeComponent(CompletableFuture.completedFuture(null));
        assertThatCode(component::close).doesNotThrowAnyException();
        assertThat(component.shutdownCalled).isTrue();
    }

    @Test
    @DisplayName("close swallows a drain failure rather than throwing")
    void closeSwallowsFailure() {
        var failed = CompletableFuture.<Void>failedFuture(new IllegalStateException("drain boom"));
        var component = new FakeComponent(failed);
        assertThatCode(component::close).doesNotThrowAnyException();
        assertThat(component.shutdownCalled).isTrue();
    }

    @Test
    @DisplayName("the default grace period is ten seconds")
    void gracePeriod() {
        assertThat(Component.DEFAULT_GRACE_PERIOD.toSeconds()).isEqualTo(10);
    }
}
