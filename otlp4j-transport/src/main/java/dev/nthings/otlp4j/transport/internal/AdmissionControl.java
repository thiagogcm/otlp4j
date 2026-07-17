package dev.nthings.otlp4j.transport.internal;

import dev.nthings.otlp4j.transport.OtlpReceiverConfig;
import dev.nthings.otlp4j.core.telemetry.GatewayMetrics;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;

/// Global admission control for a receiver: caps decoded export requests and bytes in flight
/// across every connection, complementing the per-request size cap and the gRPC per-connection
/// concurrency cap (Collector `memory_limiter`-style back-pressure).
///
/// Non-blocking: [#tryAcquire] either returns a [Permit] (closed exactly once when the request's
/// dispatch completes) or null, in which case the transport rejects retryably - gRPC
/// `UNAVAILABLE` "receiver at capacity" with an optional pushback trailer, HTTP `429` with an
/// optional `Retry-After` header (see [OtlpReceiverConfig.Builder#setThrottleRetryAfter]).
///
/// Progress guarantee: a request is always admitted when nothing else is in flight, so a single
/// request larger than the byte cap cannot starve forever. A limit of `0` disables that
/// dimension; with both disabled AND no self-observability registry, [#of] returns the shared no-op
/// [#unlimited()] instance.
///
/// Self-observability ([GatewayMetrics]): when a real (non-[GatewayMetrics#noop]) registry is
/// supplied, [#of(OtlpReceiverConfig, GatewayMetrics)] always builds a per-instance counting control (even
/// when both caps are disabled) so the most common uncapped deployment still reports the
/// `otlp4j.receiver.admission.in_flight.requests`/`.bytes` gauges and a (zero) `.rejected` counter.
/// The instance is [AutoCloseable]: the owning server closes it in `shutdown`/`shutdownNow` to
/// unregister the gauges.
public final class AdmissionControl implements AutoCloseable {

    private static final AdmissionControl UNLIMITED = new AdmissionControl();

    /// The always-admitting no-op permit; its close mutates nothing.
    private static final Permit NO_OP = UNLIMITED.new Permit(0L, true);

    private final int maxRequests;
    private final long maxBytes;
    private final @Nullable Duration retryAfterHint;
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicLong bytes = new AtomicLong();
    private final LongAdder rejected = new LongAdder();
    private final GatewayMetrics.LongCounter rejectedCounter;
    private final List<GatewayMetrics.Registration> registrations;
    private final AtomicBoolean closed = new AtomicBoolean();

    /// The shared no-op instance: no caps, no instruments, `tryAcquire` short-circuits.
    private AdmissionControl() {
        this.maxRequests = 0;
        this.maxBytes = 0L;
        this.retryAfterHint = null;
        this.rejectedCounter = GatewayMetrics.noop().counter("otlp4j.receiver.admission.rejected", "");
        this.registrations = List.of();
    }

    /// A per-instance counting control: registers the in-flight gauges and the rejected counter
    /// against `telemetry` (which may be [GatewayMetrics#noop] when a cap is set without a registry).
    private AdmissionControl(OtlpReceiverConfig config, GatewayMetrics telemetry) {
        this.maxRequests = config.maxInFlightRequests();
        this.maxBytes = config.maxInFlightBytes();
        this.retryAfterHint = config.throttleRetryAfter();
        this.rejectedCounter = telemetry.counter("otlp4j.receiver.admission.rejected",
                "requests rejected at admission because the receiver was at capacity");
        this.registrations = List.of(
                telemetry.gauge("otlp4j.receiver.admission.in_flight.requests",
                        "decoded export requests currently admitted and in flight", requests::get),
                telemetry.gauge("otlp4j.receiver.admission.in_flight.bytes",
                        "decoded request bytes currently admitted and in flight", bytes::get));
    }

    /// Builds admission control from the config alone, with no self-observability
    /// ([GatewayMetrics#noop]); returns [#unlimited()] when both limits are disabled.
    ///
    /// @param config the receiver config
    /// @return the admission control for this receiver
    public static AdmissionControl of(OtlpReceiverConfig config) {
        return of(config, GatewayMetrics.noop());
    }

    /// Builds admission control, reporting to `telemetry`. Returns the shared no-op [#unlimited()]
    /// only when both caps are disabled AND `telemetry` is [GatewayMetrics#noop]; otherwise it builds
    /// a per-instance counting control that registers the in-flight gauges and a rejected counter, so
    /// an uncapped-but-observed deployment still sees its admission health.
    ///
    /// @param config    the receiver config
    /// @param telemetry the self-observability registry
    /// @return the admission control for this receiver
    public static AdmissionControl of(OtlpReceiverConfig config, GatewayMetrics telemetry) {
        boolean uncapped = config.maxInFlightRequests() == 0 && config.maxInFlightBytes() == 0L;
        if (uncapped && telemetry == GatewayMetrics.noop()) {
            return UNLIMITED;
        }
        return new AdmissionControl(config, telemetry);
    }

    /// The shared no-op instance: every acquire succeeds, nothing is counted.
    ///
    /// @return the unlimited instance
    public static AdmissionControl unlimited() {
        return UNLIMITED;
    }

    /// Attempts to admit one request. Never blocks. The size supplier is invoked at most once,
    /// and only when a limit is configured, so callers may pass a lazily computed size (e.g.
    /// `request::getSerializedSize`).
    ///
    /// @param requestBytes supplies the decoded size of the request
    /// @return a permit to close when the request's dispatch completes, or null when the
    ///         receiver is at capacity
    public @Nullable Permit tryAcquire(LongSupplier requestBytes) {
        if (this == UNLIMITED) {
            return NO_OP;
        }
        var size = requestBytes.getAsLong();
        while (true) {
            var currentRequests = requests.get();
            if (maxRequests > 0 && currentRequests >= maxRequests) {
                recordRejected();
                return null;
            }
            if (!requests.compareAndSet(currentRequests, currentRequests + 1)) {
                continue;
            }
            // Add first, roll back on overshoot: two concurrent admits cannot both slip under
            // the cap through a stale read. Progress guarantee: the request that took the count
            // from 0 to 1 is admitted regardless of the byte cap.
            var admitWhenIdle = currentRequests == 0;
            var newBytes = bytes.addAndGet(size);
            if (!admitWhenIdle && maxBytes > 0 && newBytes > maxBytes) {
                bytes.addAndGet(-size);
                requests.decrementAndGet();
                recordRejected();
                return null;
            }
            return new Permit(size, false);
        }
    }

    /// Records one admission rejection against both the getter's [LongAdder] and the metrics counter.
    private void recordRejected() {
        rejected.increment();
        rejectedCounter.add(1);
    }

    /// Unregisters the in-flight gauges (idempotent). The shared [#unlimited()] instance registers no
    /// gauges, so its close is a no-op; a counting instance is closed by the owning server on
    /// shutdown so a dead receiver is not polled forever.
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            registrations.forEach(GatewayMetrics.Registration::close);
        }
    }

    /// The configured retry-after hint for admission rejections, or null for none.
    ///
    /// @return the hint attached to "at capacity" rejections
    public @Nullable Duration retryAfterHint() {
        return retryAfterHint;
    }

    /// Requests currently admitted and not yet released.
    ///
    /// @return the in-flight request count
    public int inFlightRequests() {
        return requests.get();
    }

    /// Decoded bytes currently admitted and not yet released.
    ///
    /// @return the in-flight byte count
    public long inFlightBytes() {
        return bytes.get();
    }

    /// Total requests rejected at admission since this receiver started.
    ///
    /// @return the rejection count
    public long rejectedCount() {
        return rejected.sum();
    }

    /// One admitted request's slot; [#close()] releases it exactly once (idempotent).
    public final class Permit implements AutoCloseable {

        private final long permitBytes;
        private final boolean noOp;
        private final AtomicBoolean released = new AtomicBoolean();

        private Permit(long permitBytes, boolean noOp) {
            this.permitBytes = permitBytes;
            this.noOp = noOp;
        }

        @Override
        public void close() {
            if (noOp || released.getAndSet(true)) {
                return;
            }
            requests.decrementAndGet();
            bytes.addAndGet(-permitBytes);
        }
    }
}
