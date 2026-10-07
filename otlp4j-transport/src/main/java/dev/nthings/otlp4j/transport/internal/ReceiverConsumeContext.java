package dev.nthings.otlp4j.transport.internal;

import dev.nthings.otlp4j.core.consumer.ClientMetadata;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.Deadline;
import dev.nthings.otlp4j.transport.OtlpReceiverConfig;
import io.grpc.Metadata;
import com.linecorp.armeria.common.HttpHeaders;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;
import com.linecorp.armeria.server.ServiceRequestContext;

/** Bridges Armeria and gRPC request state into the core consume context. */
public final class ReceiverConsumeContext implements ConsumeContext {

    private static final Duration DEFAULT_BUDGET = OtlpReceiverConfig.DEFAULT_MAX_CLIENT_TIMEOUT;
    private static final ScheduledThreadPoolExecutor DEADLINE_EXECUTOR =
            new ScheduledThreadPoolExecutor(1,
                    Thread.ofPlatform().daemon().name("otlp4j-receiver-deadline-", 0).factory());

    static {
        DEADLINE_EXECUTOR.setRemoveOnCancelPolicy(true);
    }

    private final ClientMetadata metadata;
    private final Deadline deadline;
    private final Clock clock;
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final CompletableFuture<Void> cancellation = new CompletableFuture<>();
    private volatile ScheduledFuture<?> deadlineTask;

    ReceiverConsumeContext(
            ClientMetadata metadata,
            Deadline deadline,
            CompletableFuture<?> cancellationSource,
            Clock clock) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.deadline = Objects.requireNonNull(deadline, "deadline");
        this.clock = Objects.requireNonNull(clock, "clock");
        cancellationSource.whenComplete((ignored, failure) -> cancel());
        scheduleDeadline();
    }

    /** Creates a context from an HTTP request's headers, timeout, and cancellation future. */
    public static ReceiverConsumeContext http(ServiceRequestContext requestContext, Clock clock) {
        Objects.requireNonNull(requestContext, "requestContext");
        Objects.requireNonNull(clock, "clock");
        var timeoutMillis = requestContext.requestTimeoutMillis();
        var budget = timeoutMillis <= 0 ? DEFAULT_BUDGET : Duration.ofMillis(timeoutMillis);
        return new ReceiverConsumeContext(
                metadata(requestContext.request().headers()),
                Deadline.after(budget, clock),
                requestContext.whenRequestCancelling(),
                clock);
    }

    /** Creates a context from the native Armeria gRPC request context. */
    public static ReceiverConsumeContext grpc(
            Clock clock, Metadata metadata, ServiceRequestContext requestContext) {
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(requestContext, "requestContext");
        var timeoutMillis = requestContext.requestTimeoutMillis();
        var budget = timeoutMillis <= 0 ? DEFAULT_BUDGET : Duration.ofMillis(timeoutMillis);
        return new ReceiverConsumeContext(
                metadata(metadata),
                Deadline.after(budget, clock),
                requestContext.whenRequestCancelling(),
                clock);
    }

    /** A deterministic context for direct dispatch tests. */
    public ReceiverConsumeContext(Clock clock) {
        this(ClientMetadata.empty(), Deadline.after(DEFAULT_BUDGET, clock), new CompletableFuture<>(), clock);
    }

    @Override
    public ClientMetadata metadata() {
        return metadata;
    }

    @Override
    public Deadline deadline() {
        return deadline;
    }

    @Override
    public boolean isCancelled() {
        return cancelled.get() || deadline.isExpired(clock);
    }

    @Override
    public void onCancel(Runnable callback) {
        Objects.requireNonNull(callback, "callback");
        cancellation.whenComplete((ignored, failure) -> callback.run());
        if (isCancelled()) {
            cancel();
        }
    }

    /** Releases the deadline monitor after dispatch has completed without cancelling the request. */
    void complete() {
        var task = deadlineTask;
        if (task != null) {
            task.cancel(false);
            deadlineTask = null;
        }
    }

    private void cancel() {
        var task = deadlineTask;
        if (task != null) {
            task.cancel(false);
        }
        if (cancelled.compareAndSet(false, true)) {
            // A consumer callback must not block the shared timer or Armeria's event loop.
            Thread.ofVirtual().name("otlp4j-receiver-cancel").start(() -> cancellation.complete(null));
        }
    }

    private void scheduleDeadline() {
        if (deadline.at().equals(Instant.MAX) || cancelled.get()) {
            return;
        }
        var remaining = deadline.remaining(clock);
        if (remaining.isNegative() || remaining.isZero()) {
            cancel();
            return;
        }
        long delayMillis;
        try {
            var wholeMillis = remaining.toMillis();
            delayMillis = remaining.minusMillis(wholeMillis).isZero()
                    ? wholeMillis
                    : Math.addExact(wholeMillis, 1L);
        } catch (ArithmeticException overflow) {
            delayMillis = Long.MAX_VALUE;
        }
        deadlineTask = DEADLINE_EXECUTOR.schedule(this::cancel, delayMillis, TimeUnit.MILLISECONDS);
    }

    private static ClientMetadata metadata(HttpHeaders headers) {
        var values = new LinkedHashMap<String, List<String>>();
        headers.forEach((name, value) ->
                addHeader(values, name.toString(), value));
        return new ClientMetadata(Optional.empty(), values);
    }

    private static void addHeader(Map<String, List<String>> values, String name, String value) {
        if (name.startsWith(":")) {
            return;
        }
        values.computeIfAbsent(name.toLowerCase(Locale.ROOT), ignored -> new ArrayList<>()).add(value);
    }

    private static ClientMetadata metadata(Metadata metadata) {
        var values = new LinkedHashMap<String, List<String>>();
        for (var key : metadata.keys()) {
            // Armeria's gRPC bridge includes HTTP/2 pseudo-headers in the Metadata view. They are
            // transport framing, not application metadata, and gRPC Metadata.Key deliberately rejects
            // names beginning with ':'.
            if (key.startsWith(":") || key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
                continue;
            }
            var entry = Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER);
            var all = metadata.getAll(entry);
            if (all != null) {
                var list = new ArrayList<String>();
                all.forEach(list::add);
                values.put(key, List.copyOf(list));
            }
        }
        return new ClientMetadata(Optional.empty(), values);
    }
}
