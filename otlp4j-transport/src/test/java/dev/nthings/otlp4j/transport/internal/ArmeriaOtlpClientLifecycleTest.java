package dev.nthings.otlp4j.transport.internal;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.transport.OtlpExporterConfig;
import dev.nthings.otlp4j.transport.Tls;
import dev.nthings.otlp4j.testing.conformance.TlsTestMaterial;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Client shutdown and incomplete mutual-TLS material.
@Timeout(60)
@DisplayName("ArmeriaOtlpClient and ArmeriaTls")
class ArmeriaOtlpClientLifecycleTest {

    private static OtlpExporterConfig config(Tls tls) {
        return OtlpExporterConfig.builder(4317).endpoint("localhost", 4317).tls(tls).build();
    }

    private static void startAndShutdown(ArmeriaOtlpClient client) {
        client.start();
        client.shutdown(Duration.ofSeconds(5)).toCompletableFuture().join();
    }

    @DisplayName("a plaintext gRPC client builds and drains")
    @Test
    void plaintextGrpcDrains() {
        var client = new ArmeriaOtlpClient(config(Tls.disabled()), Protocol.GRPC);
        assertThatCode(() -> startAndShutdown(client)).doesNotThrowAnyException();
        assertThatThrownBy(client::start).isInstanceOf(IllegalStateException.class);
        assertThatCode(() -> client.shutdown(Duration.ZERO).toCompletableFuture().join())
                .doesNotThrowAnyException();
    }

    @DisplayName("incomplete client mutual-TLS material is rejected at startup")
    @Test
    void incompleteMutualRejected() {
        var tls = Tls.custom(TlsTestMaterial.clientCertPem(), (byte[]) null, TlsTestMaterial.caPem());
        assertThatThrownBy(() -> {
            var client = new ArmeriaOtlpClient(config(tls), Protocol.GRPC);
            client.start();
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("certificate and key");
    }
}
