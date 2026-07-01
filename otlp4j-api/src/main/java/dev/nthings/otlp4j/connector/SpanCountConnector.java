package dev.nthings.otlp4j.connector;

import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.MetricSink;
import dev.nthings.otlp4j.pipeline.TraceSink;

/// The span-count connector as a [TraceSink]: counts spans per batch and emits
/// `otlp4j.connector.span.count` to its downstream metric sink, whose lifecycle it cascades.
final class SpanCountConnector extends CountConnector<TracesData> implements TraceSink {

    SpanCountConnector(MetricSink downstream, FailurePolicy policy) {
        super(
                downstream,
                policy,
                "otlp4j.connector.span.count",
                "Items observed by the span count connector",
                TracesData::spanCount);
    }
}
