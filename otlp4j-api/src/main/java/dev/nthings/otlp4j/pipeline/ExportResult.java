package dev.nthings.otlp4j.pipeline;

/// The outcome of handing one telemetry batch to a [TelemetryConsumer].
///
/// Maps to OTLP `partial_success`: [#success()] accepts the batch,
/// [#partialSuccess(long, String)] reports item-level rejection, and [#rejected(String)] reports
/// whole-batch rejection. Throw from a consumer for a transport-level failure.
public record ExportResult(long rejectedCount, String message) {

    private static final ExportResult SUCCESS = new ExportResult(0, "");

    public ExportResult {
        if (rejectedCount < 0) {
            throw new IllegalArgumentException("rejectedCount must be >= 0");
        }
        message = message == null ? "" : message;
    }

    public static ExportResult success() {
        return SUCCESS;
    }

    public static ExportResult partialSuccess(long rejectedCount, String message) {
        return new ExportResult(rejectedCount, message);
    }

    public static ExportResult rejected(String message) {
        return new ExportResult(0, message);
    }

    /// True when nothing was rejected and no warning message was attached.
    public boolean isFullSuccess() {
        return rejectedCount == 0 && message.isEmpty();
    }

    /// Combines two results, as when a component fans a batch out to several downstreams: the
    /// rejected counts add up and non-empty messages are joined.
    public ExportResult and(ExportResult other) {
        if (this.isFullSuccess()) {
            return other;
        }
        if (other.isFullSuccess()) {
            return this;
        }
        var combined =
                message.isEmpty()
                        ? other.message
                        : other.message.isEmpty() ? message : message + "; " + other.message;
        return new ExportResult(rejectedCount + other.rejectedCount, combined);
    }
}
