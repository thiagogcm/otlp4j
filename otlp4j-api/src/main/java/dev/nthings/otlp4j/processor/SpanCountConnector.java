package dev.nthings.otlp4j.processor;

import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.pipeline.TracesSink;

/// The span-count connector as a [TracesSink]: counts spans per batch and emits
/// `otlp4j.connector.span.count` to its downstream metric sink, whose lifecycle it cascades.
final class SpanCountConnector extends CountConnector<TracesData> implements TracesSink {

    SpanCountConnector(Sink<? super MetricsData> downstream, FailurePolicy policy) {
        super(
                downstream,
                policy,
                "otlp4j.connector.span.count",
                "Items observed by the span count connector",
                TracesData::spanCount);
    }
}
