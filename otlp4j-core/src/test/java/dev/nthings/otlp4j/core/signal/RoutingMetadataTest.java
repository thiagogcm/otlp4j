package dev.nthings.otlp4j.core.signal;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.TracesData;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for resource-only routing metadata on typed payloads.
class RoutingMetadataTest {

    @Test
    @DisplayName("domain payload exposes immutable resource attributes")
    void domainMetadata() {
        var data = TracesData.of(
                Resource.of(Attributes.builder().put("service.name", "checkout").build()),
                dev.nthings.otlp4j.model.InstrumentationScope.EMPTY,
                List.of());
        var metadata = Payload.of(SignalDescriptor.TRACES, data).routingMetadata();

        assertThat(metadata.resourceAttribute("service.name")).contains("checkout");
        assertThat(metadata.resourceAttribute("absent")).isEmpty();
        assertThat(metadata.resourceAttributeKeys()).containsExactly("service.name");
    }

    @Test
    @DisplayName("payload metadata contains no signal or duplicate item-count state")
    void payloadOwnsIdentityAndCount() {
        var payload = Payload.of(SignalDescriptor.TRACES, new TracesData(List.of()));
        assertThat(payload.signal()).isSameAs(SignalDescriptor.TRACES);
        assertThat(payload.itemCount()).isZero();
    }
}
