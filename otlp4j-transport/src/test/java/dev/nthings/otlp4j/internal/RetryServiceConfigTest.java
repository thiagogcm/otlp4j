package dev.nthings.otlp4j.internal;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.spi.RetryPolicy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/// Unit tests for the gRPC retry service-config mapping.
class RetryServiceConfigTest {

    @Test
    void emitsOneMethodConfigPerServiceWithTheRetryableCodes() {
        var config = RetryServiceConfig.build(
                new RetryPolicy(3, Duration.ofMillis(50), Duration.ofMillis(200)),
                List.of("svc.A", "svc.B"));

        var methodConfigs = (List<?>) config.get("methodConfig");
        assertThat(methodConfigs).hasSize(2);

        var retry = retryPolicyOf(methodConfigs, 0);
        assertThat(retry.get("maxAttempts")).isEqualTo(3.0d);
        assertThat(retry.get("initialBackoff")).isEqualTo("0.05s");
        assertThat(retry.get("maxBackoff")).isEqualTo("0.2s");
        assertThat(retry.get("backoffMultiplier")).isEqualTo(2.0d);
        @SuppressWarnings("unchecked")
        var codes = (List<String>) retry.get("retryableStatusCodes");
        assertThat(codes).contains("UNAVAILABLE", "DEADLINE_EXCEEDED");
    }

    private static Map<?, ?> retryPolicyOf(List<?> methodConfigs, int index) {
        return (Map<?, ?>) ((Map<?, ?>) methodConfigs.get(index)).get("retryPolicy");
    }
}
