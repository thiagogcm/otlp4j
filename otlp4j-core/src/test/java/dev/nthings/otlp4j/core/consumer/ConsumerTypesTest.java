package dev.nthings.otlp4j.core.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.Principal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for the consumer-package value types: [ClientMetadata], [ConsumeContext], and
/// [Deadline].
class ConsumerTypesTest {

    @Test
    @DisplayName("client metadata defensively copies its headers and offers an empty instance")
    void clientMetadata() {
        var headers = new HashMap<String, List<String>>();
        var tenantValues = new ArrayList<>(List.of("acme"));
        headers.put("x-tenant", tenantValues);
        var metadata = new ClientMetadata(Optional.empty(), headers);
        headers.put("x-added", List.of("late"));
        assertThat(metadata.headers()).containsOnlyKeys("x-tenant");

        tenantValues.add("mutated");
        assertThat(metadata.headers().get("x-tenant")).containsExactly("acme");

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
    @DisplayName("an already expired context invokes cancellation callbacks immediately")
    void expiredContextInvokesCancellation() {
        var expired = ConsumeContext.of(
                ClientMetadata.empty(), new Deadline(Instant.now().minusSeconds(1)));
        var callbackCalled = new AtomicBoolean();
        expired.onCancel(() -> callbackCalled.set(true));
        assertThat(expired.isCancelled()).isTrue();
        assertThat(callbackCalled).isTrue();
    }

    @Test
    @DisplayName("deadline cancellation invokes every callback once without a polling call")
    void scheduledCancellationCallbacks() throws InterruptedException {
        var context = ConsumeContext.of(
                ClientMetadata.empty(), Deadline.after(Duration.ofMillis(80), Clock.systemUTC()));
        var callbacks = new CountDownLatch(2);
        var calls = new AtomicInteger();
        context.onCancel(() -> {
            calls.incrementAndGet();
            callbacks.countDown();
            throw new IllegalStateException("callback failure");
        });
        context.onCancel(() -> {
            calls.incrementAndGet();
            callbacks.countDown();
        });

        assertThat(callbacks.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(calls.get()).isEqualTo(2);
        assertThat(context.isCancelled()).isTrue();
        assertThat(calls.get()).isEqualTo(2);

        var lateCalls = new AtomicInteger();
        context.onCancel(lateCalls::incrementAndGet);
        assertThat(lateCalls.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("a blocked cancellation callback does not delay unrelated deadlines")
    void blockedCallbackDoesNotBlockDeadlineScheduler() throws InterruptedException {
        var first = ConsumeContext.of(
                ClientMetadata.empty(), Deadline.after(Duration.ofMillis(80), Clock.systemUTC()));
        var second = ConsumeContext.of(
                ClientMetadata.empty(), Deadline.after(Duration.ofMillis(160), Clock.systemUTC()));
        var firstStarted = new CountDownLatch(1);
        var releaseFirst = new CountDownLatch(1);
        var secondCancelled = new CountDownLatch(1);

        first.onCancel(() -> {
            firstStarted.countDown();
            try {
                releaseFirst.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        });
        second.onCancel(secondCancelled::countDown);

        try {
            assertThat(firstStarted.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(secondCancelled.await(2, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseFirst.countDown();
        }
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
