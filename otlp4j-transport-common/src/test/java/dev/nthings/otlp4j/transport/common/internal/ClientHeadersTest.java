package dev.nthings.otlp4j.transport.common.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Per-export header resolution")
class ClientHeadersTest {

    @DisplayName("returns the constant map unchanged when no supplier is set")
    @Test
    void noSupplierReturnsConstant() {
        var constant = Map.of("x-tenant", "abc");
        assertThat(ClientHeaders.resolve(constant, null)).isSameAs(constant);
    }

    @DisplayName("overlays the supplier over the constant map, supplier winning per key")
    @Test
    void supplierOverlaysConstant() {
        var constant = Map.of("x-tenant", "abc", "authorization", "Bearer stale");
        var merged = ClientHeaders.resolve(constant, () -> Map.of("authorization", "Bearer fresh"));

        assertThat(merged)
                .containsEntry("x-tenant", "abc") // static key preserved
                .containsEntry("authorization", "Bearer fresh"); // supplier wins per key
    }

    @DisplayName("evaluates the supplier on each call (rotating credential)")
    @Test
    void reevaluatesSupplier() {
        var counter = new AtomicInteger();
        Supplier<Map<String, String>> supplier =
                () -> Map.of("authorization", "Bearer " + counter.incrementAndGet());

        assertThat(ClientHeaders.resolve(Map.of(), supplier)).containsEntry("authorization", "Bearer 1");
        assertThat(ClientHeaders.resolve(Map.of(), supplier)).containsEntry("authorization", "Bearer 2");
    }

    @DisplayName("a supplier injecting an invalid header fails the resolve")
    @Test
    void invalidSupplierHeaderFails() {
        assertThatThrownBy(() -> ClientHeaders.resolve(Map.of(), () -> Map.of("evil", "a\r\nb")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("evil");
        assertThatThrownBy(() -> ClientHeaders.resolve(Map.of(), () -> Map.of("bad name", "v")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
