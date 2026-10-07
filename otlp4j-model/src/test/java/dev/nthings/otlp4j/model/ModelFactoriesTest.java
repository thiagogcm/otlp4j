package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Attribute enrichment retains source metadata without mutating the original values.
@DisplayName("Resource and scope enrichment")
class ModelFactoriesTest {

    @DisplayName("Resource.withAttribute adds or replaces one attribute, preserving dropped count")
    @Test
    void resourceWithAttributeAddsOrReplacesOneAttribute() {
        var base = new Resource(Attributes.builder().put("service.name", "old").build(), 3);

        var enriched = base.withAttribute("service.name", AttributeValue.of("checkout"))
                .withAttribute("env", AttributeValue.of("prod"));

        assertThat(enriched.attributes().getString("service.name")).isEqualTo("checkout");
        assertThat(enriched.attributes().getString("env")).isEqualTo("prod");
        assertThat(enriched.droppedAttributesCount()).isEqualTo(3);
        assertThat(base.attributes().getString("service.name"))
                .as("withAttribute must not mutate the source resource")
                .isEqualTo("old");
    }

    @DisplayName("InstrumentationScope.withAttribute adds an attribute, preserving name/version/dropped count")
    @Test
    void instrumentationScopeWithAttributeAddsAnAttribute() {
        var base = new InstrumentationScope("lib", "2.0", Attributes.empty(), 1);

        var enriched = base.withAttribute("team", AttributeValue.of("payments"));

        assertThat(enriched.name()).isEqualTo("lib");
        assertThat(enriched.version()).isEqualTo("2.0");
        assertThat(enriched.droppedAttributesCount()).isEqualTo(1);
        assertThat(enriched.attributes().getString("team")).isEqualTo("payments");
        assertThat(base.attributes().contains("team"))
                .as("withAttribute must not mutate the source scope")
                .isFalse();
    }
}
