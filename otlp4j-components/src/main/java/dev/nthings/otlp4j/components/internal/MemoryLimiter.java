package dev.nthings.otlp4j.components.internal;

import java.util.function.LongSupplier;

/// The shared limiter core behind the built-in memory limiter processor: a small state machine over a
/// used-memory gauge and two byte thresholds. It is module-internal so a future extension form can wrap
/// the same logic without the state machine becoming a public contract.
///
/// The gauge is a [LongSupplier] of the currently used bytes, defaulting to the JVM heap reading
/// (`Runtime.totalMemory() - Runtime.freeMemory()`) but injectable, so a test drives deterministic
/// thresholds without provoking real heap pressure. [#refresh] samples the gauge once and recomputes
/// the state; the owning component schedules it every check interval, and [#state] is read on the hot
/// consume path without sampling. The mapping is a plain threshold with automatic recovery: at or above
/// the hard threshold the state is [State#HARD], at or above the soft threshold but below the hard one
/// it is [State#SOFT], and below the soft threshold it is [State#NORMAL], so a later sample that reads
/// used memory back under the soft threshold returns the limiter to normal on its own.
public final class MemoryLimiter {

    /// The sampled memory-pressure state.
    public enum State {

        /// Used memory is below the soft threshold; payloads are admitted unchanged.
        NORMAL,

        /// Used memory is at or above the soft threshold but below the hard one; payloads are admitted
        /// and the owning component requests a best-effort collection.
        SOFT,

        /// Used memory is at or above the hard threshold; the owning component refuses ingress.
        HARD
    }

    private final LongSupplier usedBytes;
    private final long softLimitBytes;
    private final long hardLimitBytes;

    private volatile State state = State.NORMAL;

    /// Builds a limiter reading `usedBytes`, comparing against the two thresholds.
    ///
    /// @param usedBytes      the used-memory gauge, in bytes
    /// @param softLimitBytes the soft threshold, in bytes, at or above which the state is [State#SOFT]
    /// @param hardLimitBytes the hard threshold, in bytes, at or above which the state is [State#HARD]
    public MemoryLimiter(LongSupplier usedBytes, long softLimitBytes, long hardLimitBytes) {
        if (usedBytes == null) {
            throw new IllegalArgumentException("usedBytes gauge must be non-null");
        }
        if (softLimitBytes > hardLimitBytes) {
            throw new IllegalArgumentException(
                    "softLimitBytes must not exceed hardLimitBytes, got soft=" + softLimitBytes
                            + " hard=" + hardLimitBytes);
        }
        this.usedBytes = usedBytes;
        this.softLimitBytes = softLimitBytes;
        this.hardLimitBytes = hardLimitBytes;
    }

    /// The JVM heap gauge used by default: currently used heap in bytes.
    ///
    /// @return a supplier of `Runtime.totalMemory() - Runtime.freeMemory()`
    public static LongSupplier heapGauge() {
        return () -> {
            var runtime = Runtime.getRuntime();
            return runtime.totalMemory() - runtime.freeMemory();
        };
    }

    /// Samples the gauge once and recomputes the state, returning the new state.
    ///
    /// @return the state after this sample
    public State refresh() {
        var used = usedBytes.getAsLong();
        var next = used >= hardLimitBytes
                ? State.HARD
                : used >= softLimitBytes ? State.SOFT : State.NORMAL;
        state = next;
        return next;
    }

    /// The most recently sampled state, read without sampling.
    ///
    /// @return the current state
    public State state() {
        return state;
    }

    /// The soft threshold, in bytes.
    ///
    /// @return the soft threshold
    public long softLimitBytes() {
        return softLimitBytes;
    }

    /// The hard threshold, in bytes.
    ///
    /// @return the hard threshold
    public long hardLimitBytes() {
        return hardLimitBytes;
    }
}
