package dev.nthings.otlp4j.core.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.Principal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for the consumer-package value types: [ConsumerCapabilities], [ClientMetadata], and
/// [Deadline].
class ConsumerTypesTest {

    @Test
    @DisplayName("consumer capabilities expose the mutation flag and shared instances")
    void capabilities() {
        assertThat(ConsumerCapabilities.nonMutating().mutatesData()).isFalse();
        assertThat(ConsumerCapabilities.mutating().mutatesData()).isTrue();
        assertThat(new ConsumerCapabilities(true)).isEqualTo(ConsumerCapabilities.mutating());
    }

    @Test
    @DisplayName("client metadata defensively copies its headers and offers an empty instance")
    void clientMetadata() {
        var headers = new java.util.HashMap<String, List<String>>();
        headers.put("x-tenant", List.of("acme"));
        var metadata = new ClientMetadata(Optional.empty(), headers);
        headers.put("x-added", List.of("late"));
        assertThat(metadata.headers()).containsOnlyKeys("x-tenant");

        Principal principal = () -> "svc";
        var authed = new ClientMetadata(Optional.of(principal), Map.of());
        assertThat(authed.principal()).containsSame(principal);

        assertThat(ClientMetadata.empty().principal()).isEmpty();
        assertThat(ClientMetadata.empty().headers()).isEmpty();

        assertThatThrownBy(() -> new ClientMetadata(null, Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ClientMetadata(Optional.empty(), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("a deadline measures remaining time and expiry against a clock")
    void deadline() {
        var now = Instant.parse("2026-07-06T00:00:00Z");
        var clock = Clock.fixed(now, ZoneOffset.UTC);

        var deadline = Deadline.after(Duration.ofSeconds(30), clock);
        assertThat(deadline.at()).isEqualTo(now.plusSeconds(30));
        assertThat(deadline.remaining(clock)).isEqualTo(Duration.ofSeconds(30));
        assertThat(deadline.isExpired(clock)).isFalse();

        var later = Clock.fixed(now.plusSeconds(31), ZoneOffset.UTC);
        assertThat(deadline.isExpired(later)).isTrue();
        assertThat(deadline.remaining(later)).isNegative();

        assertThatThrownBy(() -> new Deadline(null)).isInstanceOf(IllegalArgumentException.class);
    }
}
