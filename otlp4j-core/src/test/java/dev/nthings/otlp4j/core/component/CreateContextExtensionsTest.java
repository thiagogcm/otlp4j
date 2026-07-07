package dev.nthings.otlp4j.core.component;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for the additive [CreateContext] extensions component: the four-argument canonical
/// constructor carries the resolver, the retained convenience constructors default it to the empty
/// view, and a null resolver is rejected.
class CreateContextExtensionsTest {

    private static final ComponentId ID = new ComponentId("otlp", "");
    private static final ComponentContext BASE =
            new ComponentContext(GatewayMetrics.noop(), Clock.systemUTC());

    @Test
    @DisplayName("the canonical constructor carries the extensions resolver")
    void canonicalCarriesExtensions() {
        var id = new ComponentId("bearertokenauth", "");
        Extension extension = new FakeExtension();
        Extensions extensions = componentId -> Optional.of(extension);
        var ctx = new CreateContext(ID, BASE, new SharedComponents(), extensions);

        assertThat(ctx.extensions()).isSameAs(extensions);
        assertThat(ctx.extensions().get(id)).containsSame(extension);
    }

    @Test
    @DisplayName("the convenience constructors default the resolver to the empty view")
    void convenienceConstructorsDefaultToEmpty() {
        var threeArg = new CreateContext(ID, BASE, new SharedComponents());
        var twoArg = new CreateContext(ID, BASE);

        assertThat(threeArg.extensions()).isSameAs(Extensions.empty());
        assertThat(twoArg.extensions()).isSameAs(Extensions.empty());
        assertThat(twoArg.sharedComponents()).isNotNull();
    }

    @Test
    @DisplayName("a null extensions resolver is rejected")
    void nullExtensionsRejected() {
        assertThatThrownBy(() -> new CreateContext(ID, BASE, new SharedComponents(), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("extensions");
    }

    private static final class FakeExtension implements Extension {
        @Override
        public CompletionStage<Void> start(ComponentContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext context) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
