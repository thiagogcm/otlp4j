package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.spi.ClientTransportConfig;
import dev.nthings.otlp4j.spi.Compression;
import dev.nthings.otlp4j.spi.RetryPolicy;
import dev.nthings.otlp4j.spi.ServerTransportConfig;
import dev.nthings.otlp4j.spi.Tls;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ConfigRecordsTest {

    @Test
    void clientConfigDefaults() {
        var c = ClientTransportConfig.builder().build();
        assertThat(c.host()).isEqualTo("localhost");
        assertThat(c.port()).isEqualTo(4317);
        assertThat(c.timeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(c.tls()).isEqualTo(Tls.Disabled.instance());
        assertThat(c.compression()).isEqualTo(Compression.NONE);
        assertThat(c.retry()).isEqualTo(RetryPolicy.none());
        assertThat(c.headers()).isEmpty();
    }

    @Test
    void clientConfigBuilderFluentOverrides() {
        var c = ClientTransportConfig.builder()
                .endpoint("collector.example.com", 4318)
                .header("x-tenant", "abc")
                .compression(Compression.GZIP)
                .timeout(Duration.ofSeconds(3))
                .build();
        assertThat(c.host()).isEqualTo("collector.example.com");
        assertThat(c.port()).isEqualTo(4318);
        assertThat(c.headers()).containsEntry("x-tenant", "abc");
        assertThat(c.compression()).isEqualTo(Compression.GZIP);
        assertThat(c.timeout()).isEqualTo(Duration.ofSeconds(3));
    }

    @Test
    void clientConfigRejectsZeroTimeout() {
        assertThatThrownBy(() -> ClientTransportConfig.builder().timeout(Duration.ZERO).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void serverConfigDefaults() {
        var c = ServerTransportConfig.builder().build();
        assertThat(c.bindHost()).isEqualTo("0.0.0.0");
        assertThat(c.port()).isEqualTo(4317);
        assertThat(c.tls()).isEqualTo(Tls.Disabled.instance());
    }

    @Test
    void serverConfigRejectsBadPort() {
        assertThatThrownBy(() -> ServerTransportConfig.builder().port(70000).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void retryPolicyValidates() {
        assertThatThrownBy(() -> new RetryPolicy(0, Duration.ZERO, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void tlsFamilyHasSharedInstances() {
        assertThat(Tls.Disabled.instance()).isSameAs(Tls.Disabled.instance());
        assertThat(Tls.SystemTrust.instance()).isSameAs(Tls.SystemTrust.instance());
    }
}
