package dev.nthings.otlp4j.core.result;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * A bounded summary of work observed while flushing a pipeline or component.
 *
 * <p>Partial success is terminal (it is not retried): its rejected item count remains observable and
 * makes this report unsuccessful. A whole-batch failure or an admission drop also makes the report
 * unsuccessful. Failure details are intentionally bounded to the first cause; implementations may
 * retain additional counters without retaining an unbounded exception list.
 *
 * @param rejectedItems items reported as partially rejected by downstream
 * @param droppedItems items dropped by a bounded queue
 * @param failedBatches whole batches that failed downstream
 * @param firstFailure the first failure cause, if one was available
 */
public record FlushReport(
        long rejectedItems,
        long droppedItems,
        long failedBatches,
        @Nullable Throwable firstFailure) {

    public FlushReport {
        if (rejectedItems < 0) {
            throw new IllegalArgumentException("rejectedItems must be non-negative");
        }
        if (droppedItems < 0) {
            throw new IllegalArgumentException("droppedItems must be non-negative");
        }
        if (failedBatches < 0) {
            throw new IllegalArgumentException("failedBatches must be non-negative");
        }
    }

    /** An empty, successful flush report. */
    public static FlushReport empty() {
        return new FlushReport(0, 0, 0, null);
    }

    /**
     * Combines two reports, retaining the first non-null failure and summing all counters.
     *
     * @param other the report to add
     * @return the combined report
     */
    public FlushReport plus(FlushReport other) {
        Objects.requireNonNull(other, "other");
        return new FlushReport(
                Math.addExact(rejectedItems, other.rejectedItems),
                Math.addExact(droppedItems, other.droppedItems),
                Math.addExact(failedBatches, other.failedBatches),
                firstFailure != null ? firstFailure : other.firstFailure);
    }

    /**
     * Whether no item was rejected or dropped and no batch failed.
     *
     * @return true when the flush completed without rejected items, drops, or whole-batch failure
     */
    public boolean isSuccess() {
        return rejectedItems == 0 && droppedItems == 0 && failedBatches == 0 && firstFailure == null;
    }
}
