package dev.nthings.otlp4j.transport.common;

import dev.nthings.otlp4j.config.ServerConfig;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.jspecify.annotations.Nullable;

/// Global admission control for a receiver: caps decoded export requests and bytes in flight
/// across every connection, complementing the per-request size cap and the gRPC per-connection
/// concurrency cap (Collector `memory_limiter`-style back-pressure).
///
/// Non-blocking: [#tryAcquire] either returns a [Permit] (closed exactly once when the request's
/// dispatch completes) or null, in which case the transport rejects retryably - gRPC
/// `UNAVAILABLE` "receiver at capacity" with an optional pushback trailer, HTTP `429` with an
/// optional `Retry-After` header (see [ServerConfig.Builder#setThrottleRetryAfter]).
///
/// Progress guarantee: a request is always admitted when nothing else is in flight, so a single
/// request larger than the byte cap cannot starve forever. A limit of `0` disables that
/// dimension; with both disabled, [#of] returns the shared no-op [#unlimited()] instance.
public final class AdmissionControl {

    private static final AdmissionControl UNLIMITED = new AdmissionControl(0, 0L, null);

    private final int maxRequests;
    private final long maxBytes;
    private final @Nullable Duration retryAfterHint;
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicLong bytes = new AtomicLong();
    private final LongAdder rejected = new LongAdder();

    /// The always-admitting no-op permit shared by the unlimited instance.
    private final Permit unlimitedPermit = new Permit(0L, true);

    private AdmissionControl(int maxRequests, long maxBytes, @Nullable Duration retryAfterHint) {
        this.maxRequests = maxRequests;
        this.maxBytes = maxBytes;
        this.retryAfterHint = retryAfterHint;
    }

    /// Builds admission control from the config's `maxInFlightRequests`, `maxInFlightBytes`, and
    /// `throttleRetryAfter`; returns [#unlimited()] when both limits are disabled.
    ///
    /// @param config the receiver config
    /// @return the admission control for this receiver
    public static AdmissionControl of(ServerConfig config) {
        if (config.maxInFlightRequests() == 0 && config.maxInFlightBytes() == 0L) {
            return UNLIMITED;
        }
        return new AdmissionControl(
                config.maxInFlightRequests(), config.maxInFlightBytes(), config.throttleRetryAfter());
    }

    /// The shared no-op instance: every acquire succeeds, nothing is counted.
    ///
    /// @return the unlimited instance
    public static AdmissionControl unlimited() {
        return UNLIMITED;
    }

    /// Attempts to admit one request of `requestBytes` decoded bytes. Never blocks.
    ///
    /// @param requestBytes the decoded size of the request
    /// @return a permit to close when the request's dispatch completes, or null when the
    ///         receiver is at capacity
    public @Nullable Permit tryAcquire(long requestBytes) {
        if (this == UNLIMITED) {
            return unlimitedPermit;
        }
        while (true) {
            var currentRequests = requests.get();
            // Progress guarantee: with nothing in flight, admit regardless of the byte cap.
            var admitWhenIdle = currentRequests == 0;
            if (!admitWhenIdle && maxRequests > 0 && currentRequests >= maxRequests) {
                rejected.increment();
                return null;
            }
            if (!requests.compareAndSet(currentRequests, currentRequests + 1)) {
                continue;
            }
            // Add first, roll back on overshoot: two concurrent admits cannot both slip under
            // the cap through a stale read.
            var newBytes = bytes.addAndGet(requestBytes);
            if (!admitWhenIdle && maxBytes > 0 && newBytes > maxBytes) {
                bytes.addAndGet(-requestBytes);
                requests.decrementAndGet();
                rejected.increment();
                return null;
            }
            return new Permit(requestBytes, false);
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
        private final AtomicInteger released = new AtomicInteger();

        private Permit(long permitBytes, boolean noOp) {
            this.permitBytes = permitBytes;
            this.noOp = noOp;
        }

        @Override
        public void close() {
            if (noOp || released.getAndIncrement() > 0) {
                return;
            }
            requests.decrementAndGet();
            bytes.addAndGet(-permitBytes);
        }
    }
}
