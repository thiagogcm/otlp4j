package dev.nthings.otlp4j.core.component;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for [Extensions]: the empty view resolves nothing, the shared empty instance is stable,
/// and a map-backed view resolves an extension by id.
class ExtensionsTest {

    @Test
    @DisplayName("the empty view resolves no extension and is shared")
    void emptyView() {
        assertThat(Extensions.empty().get(new ComponentId("auth", ""))).isEmpty();
        assertThat(Extensions.empty()).isSameAs(Extensions.EMPTY);
    }

    @Test
    @DisplayName("a map-backed view resolves a registered extension by id and misses otherwise")
    void mapBackedView() {
        var id = new ComponentId("bearertokenauth", "");
        Extension extension = new FakeExtension();
        var map = Map.of(id, extension);
        Extensions extensions = componentId -> Optional.ofNullable(map.get(componentId));

        assertThat(extensions.get(id)).containsSame(extension);
        assertThat(extensions.get(new ComponentId("other", ""))).isEmpty();
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
