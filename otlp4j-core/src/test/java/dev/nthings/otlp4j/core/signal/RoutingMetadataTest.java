package dev.nthings.otlp4j.core.signal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for the [RoutingMetadata] contract and the [PayloadView#ROUTING_METADATA] view constant
/// it is read through. The interface has no behaviour of its own; these tests pin the view constant
/// and exercise a representative implementation so the resource-attribute routing shape is documented.
class RoutingMetadataTest {

    @Test
    @DisplayName("ROUTING_METADATA is a distinct, named view constant")
    void routingMetadataViewConstant() {
        assertThat(PayloadView.ROUTING_METADATA).hasToString("ROUTING_METADATA");
        assertThat(PayloadView.ROUTING_METADATA)
                .isNotSameAs(PayloadView.PROTO_REQUEST)
                .isNotSameAs(PayloadView.DOMAIN_IMMUTABLE);
    }

    @Test
    @DisplayName("a routing metadata reports signal, item count, and resource attributes")
    void representativeImplementation() {
        var attributes = new LinkedHashMap<String, String>();
        attributes.put("service.name", "checkout");
        attributes.put("deployment.environment", "prod");
        RoutingMetadata metadata = new Sample(SignalDescriptor.TRACES, 42, attributes);

        assertThat(metadata.signal()).isEqualTo(SignalDescriptor.TRACES);
        assertThat(metadata.itemCount()).isEqualTo(42);
        assertThat(metadata.resourceAttribute("service.name")).contains("checkout");
        assertThat(metadata.resourceAttribute("deployment.environment")).contains("prod");
        assertThat(metadata.resourceAttribute("absent")).isEmpty();
        assertThat(metadata.resourceAttributeKeys())
                .containsExactlyInAnyOrder("service.name", "deployment.environment");
    }

    private record Sample(SignalDescriptor<?> signal, long itemCount, Map<String, String> attributes)
            implements RoutingMetadata {

        @Override
        public Optional<String> resourceAttribute(String key) {
            return Optional.ofNullable(attributes.get(key));
        }

        @Override
        public Set<String> resourceAttributeKeys() {
            return attributes.keySet();
        }
    }
}
