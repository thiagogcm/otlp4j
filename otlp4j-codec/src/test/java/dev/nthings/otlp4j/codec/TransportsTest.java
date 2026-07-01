package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Transport shared plumbing")
class TransportsTest {

    @DisplayName("resolveHeaders returns the constant map unchanged when no supplier is set")
    @Test
    void resolveHeadersNoSupplier() {
        var constant = Map.of("x-tenant", "abc");
        assertThat(Transports.resolveHeaders(constant, null)).isSameAs(constant);
    }

    @DisplayName("resolveHeaders overlays the supplier over the constant map, supplier winning per key")
    @Test
    void resolveHeadersOverlaysSupplier() {
        var constant = Map.of("x-tenant", "abc", "authorization", "Bearer stale");
        var merged = Transports.resolveHeaders(constant, () -> Map.of("authorization", "Bearer fresh"));

        assertThat(merged)
                .containsEntry("x-tenant", "abc") // static key preserved
                .containsEntry("authorization", "Bearer fresh"); // supplier wins per key
    }

    @DisplayName("resolveHeaders evaluates the supplier on each call (rotating credential)")
    @Test
    void resolveHeadersReevaluatesSupplier() {
        var counter = new AtomicInteger();
        Supplier<Map<String, String>> supplier =
                () -> Map.of("authorization", "Bearer " + counter.incrementAndGet());

        assertThat(Transports.resolveHeaders(Map.of(), supplier)).containsEntry("authorization", "Bearer 1");
        assertThat(Transports.resolveHeaders(Map.of(), supplier)).containsEntry("authorization", "Bearer 2");
    }

    @DisplayName("requireCompleteClientMutualTls rejects half-specified in-memory material")
    @Test
    void requireCompleteClientMutualTlsBytes() {
        var pem = new byte[] {1, 2, 3};
        // Both present or both absent is complete.
        Transports.requireCompleteClientMutualTls(pem, pem);
        Transports.requireCompleteClientMutualTls((byte[]) null, (byte[]) null);
        // Half-specified is rejected.
        assertThatThrownBy(() -> Transports.requireCompleteClientMutualTls(pem, (byte[]) null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Transports.requireCompleteClientMutualTls((byte[]) null, pem))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
