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
    @DisplayName("a domain payload lists the attributes of every resource group in wire order")
    void domainMetadata() {
        var payments = Attributes.builder().put("service.name", "payments").build();
        var checkout = Attributes.builder().put("service.name", "checkout").put("tenant", "acme").build();
        var data = new TracesData(List.of(
                new TracesData.ResourceSpans(Resource.of(payments), "", List.of()),
                new TracesData.ResourceSpans(Resource.of(checkout), "", List.of())));
        var metadata = Payload.of(SignalDescriptor.TRACES, data).routingMetadata();

        assertThat(metadata.resources()).containsExactly(payments, checkout);
    }
}
