package dev.nthings.otlp4j.transport.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.server.ServiceRequestContext;
import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.core.consumer.Deadline;
import io.grpc.Metadata;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(5)
@DisplayName("ReceiverConsumeContext")
class ReceiverConsumeContextTest {

    private static final Clock CLOCK = Clock.systemUTC();

    @Test
    @DisplayName("an HTTP request deadline invokes each cancellation callback exactly once")
    void httpDeadlineCallbackRunsExactlyOnce() throws Exception {
        var request = httpRequest();
        request.setRequestTimeoutMillis(50);
        var context = ReceiverConsumeContext.http(request, CLOCK);
        try {
            var callbacks = new AtomicInteger();
            var cancelled = new CountDownLatch(1);
            context.onCancel(() -> {
                callbacks.incrementAndGet();
                cancelled.countDown();
            });

            assertThat(cancelled.await(2, TimeUnit.SECONDS)).isTrue();
            context.onCancel(callbacks::incrementAndGet);
            await().pollDelay(Duration.ofMillis(100)).atMost(Duration.ofMillis(400))
                    .untilAsserted(() -> assertThat(callbacks).hasValue(2));
            assertThat(context.isCancelled()).isTrue();
        } finally {
            context.complete();
        }
    }

    @Test
    @DisplayName("completing before a deadline closes the monitor without invoking cancellation")
    void completeDoesNotTriggerDeadlineCallback() {
        var context = new ReceiverConsumeContext(ClientMetadata.empty(),
                Deadline.after(Duration.ofMillis(50), CLOCK), new CompletableFuture<>(), CLOCK);
        try {
            var callbacks = new AtomicInteger();
            context.onCancel(callbacks::incrementAndGet);
            context.complete();

            await().pollDelay(Duration.ofMillis(100)).atMost(Duration.ofMillis(400))
                    .untilAsserted(() -> assertThat(callbacks).hasValue(0));
        } finally {
            context.complete();
        }
    }

    @Test
    @DisplayName("gRPC cancellation forwards ASCII metadata and filters binary metadata")
    void grpcCancellationAndMetadata() throws Exception {
        var metadata = new Metadata();
        var tenant = Metadata.Key.of("x-tenant", Metadata.ASCII_STRING_MARSHALLER);
        metadata.put(tenant, "acme");
        metadata.put(tenant, "backup");
        var binary = Metadata.Key.of("trace-bin", Metadata.BINARY_BYTE_MARSHALLER);
        metadata.put(binary, new byte[] {1, 2, 3});

        var request = httpRequest();
        var context = ReceiverConsumeContext.grpc(CLOCK, metadata, request);
        try {
            assertThat(context.metadata().headers())
                    .containsEntry("x-tenant", List.of("acme", "backup"))
                    .doesNotContainKey("trace-bin");

            var callbacks = new AtomicInteger();
            var cancelled = new CountDownLatch(1);
            context.onCancel(() -> {
                callbacks.incrementAndGet();
                cancelled.countDown();
            });
            request.cancel(new IllegalStateException("client disconnected"));

            assertThat(cancelled.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(callbacks).hasValue(1);
            assertThat(context.isCancelled()).isTrue();
        } finally {
            context.complete();
        }
    }

    @Test
    @DisplayName("a gRPC deadline is mapped and cancellation is delivered once")
    void grpcDeadlineCancelsOnce() throws Exception {
        var request = httpRequest();
        request.setRequestTimeoutMillis(50);
        var context = ReceiverConsumeContext.grpc(CLOCK, new Metadata(), request);
        try {
            var callbacks = new AtomicInteger();
            var cancelled = new CountDownLatch(1);
            context.onCancel(() -> {
                callbacks.incrementAndGet();
                cancelled.countDown();
            });

            assertThat(cancelled.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(callbacks).hasValue(1);
            assertThat(context.isCancelled()).isTrue();
        } finally {
            context.complete();
        }
    }

    @Test
    @DisplayName("HTTP metadata keeps application headers, removes pseudo-headers, and uses request timeout")
    void httpMetadataPseudoHeadersAndTimeout() throws Exception {
        var request = httpRequest();
        request.setRequestTimeoutMillis(50);
        var context = ReceiverConsumeContext.http(request, CLOCK);
        try {
            assertThat(context.metadata().headers())
                    .containsEntry("x-tenant", List.of("acme", "backup"))
                    .doesNotContainKeys(":method", ":scheme", ":authority", ":path");

            var cancelled = new CountDownLatch(1);
            context.onCancel(cancelled::countDown);
            assertThat(cancelled.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(context.isCancelled()).isTrue();
        } finally {
            context.complete();
        }
    }

    private static ServiceRequestContext httpRequest() {
        var headers = RequestHeaders.builder(HttpMethod.POST, "/v1/traces")
                .scheme("http")
                .authority("collector.example")
                .add("x-tenant", "acme")
                .add("X-Tenant", "backup")
                .build();
        return ServiceRequestContext.builder(HttpRequest.of(headers)).build();
    }
}
