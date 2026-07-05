package dev.nthings.otlp4j.pipeline.ops;

import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.Signal;
import dev.nthings.otlp4j.pipeline.LogsSink;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.pipeline.TracesSink;
import java.util.List;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/// Factories for built-in count sinks; overloads default to [FailurePolicy#BEST_EFFORT]. The
/// counts are [Connector] instances - extend that class to write your own signal-changing
/// connector.
///
/// Each returned sink is a `Lifecycle` that cascades shutdown to its `downstream` metric sink, so a
/// pipeline drains that downstream automatically when the connector is attached as a terminal or
/// fan-out peer.
public final class Connectors {

    private Connectors() {}

    /// Trace count sink emitting `otlp4j.connector.span.count` into `downstream`, best-effort.
    public static TracesSink spanCount(Sink<? super MetricsData> downstream) {
        return spanCount(downstream, FailurePolicy.BEST_EFFORT);
    }

    /// Trace count sink emitting `otlp4j.connector.span.count` into `downstream` under `policy`.
    public static TracesSink spanCount(Sink<? super MetricsData> downstream, FailurePolicy policy) {
        return new SpanCountConnector(downstream, policy);
    }

    /// Log count sink emitting `otlp4j.connector.log.record.count` into `downstream`, best-effort.
    public static LogsSink logRecordCount(Sink<? super MetricsData> downstream) {
        return logRecordCount(downstream, FailurePolicy.BEST_EFFORT);
    }

    /// Log count sink emitting `otlp4j.connector.log.record.count` into `downstream` under `policy`.
    public static LogsSink logRecordCount(Sink<? super MetricsData> downstream, FailurePolicy policy) {
        return new LogRecordCountConnector(downstream, policy);
    }

    /// A connector applying `derive` to convert `in` batches into `out` batches, delivered
    /// best-effort to one downstream. Adding a conversion is this one call, no subclass. A null
    /// derive result emits nothing and accepts the input (the [Connector#derive] contract).
    ///
    /// @param name       the connector name, used in log and rejection messages
    /// @param in         the input signal
    /// @param out        the derived output signal
    /// @param derive     maps an input batch to zero (null) or one output batch
    /// @param downstream the sink receiving derived batches
    /// @param <I>        the input signal type
    /// @param <O>        the output signal type
    /// @return the connector
    public static <I, O> Connector<I, O> of(
            String name, Signal<I> in, Signal<O> out,
            Function<I, @Nullable O> derive, Sink<? super O> downstream) {
        return of(name, in, out, derive, List.of(downstream), FailurePolicy.BEST_EFFORT);
    }

    /// As [#of(String,Signal,Signal,Function,Sink)] with an explicit [FailurePolicy].
    ///
    /// @param name       the connector name, used in log and rejection messages
    /// @param in         the input signal
    /// @param out        the derived output signal
    /// @param derive     maps an input batch to zero (null) or one output batch
    /// @param downstream the sink receiving derived batches
    /// @param policy     how a derived-delivery failure reflects on the input batch
    /// @param <I>        the input signal type
    /// @param <O>        the output signal type
    /// @return the connector
    public static <I, O> Connector<I, O> of(
            String name, Signal<I> in, Signal<O> out,
            Function<I, @Nullable O> derive, Sink<? super O> downstream, FailurePolicy policy) {
        return of(name, in, out, derive, List.of(downstream), policy);
    }

    /// As [#of(String,Signal,Signal,Function,Sink)], fanning the derived batch to every downstream
    /// (at least one), merging with FanOut semantics. A peer [Error] with two or more downstreams is
    /// isolated (see [Connector]).
    ///
    /// @param name        the connector name, used in log and rejection messages
    /// @param in          the input signal
    /// @param out         the derived output signal
    /// @param derive      maps an input batch to zero (null) or one output batch
    /// @param downstreams the sinks receiving derived batches (at least one)
    /// @param policy      how a merged derived-delivery failure reflects on the input batch
    /// @param <I>         the input signal type
    /// @param <O>         the output signal type
    /// @return the connector
    public static <I, O> Connector<I, O> of(
            String name, Signal<I> in, Signal<O> out,
            Function<I, @Nullable O> derive, List<? extends Sink<? super O>> downstreams, FailurePolicy policy) {
        return new FunctionConnector<>(name, in, out, downstreams, policy, derive);
    }

    /// Span count sink routing one count to several downstream metric sinks (at least one) under
    /// `policy`, merging with FanOut semantics.
    ///
    /// @param downstreams the metric sinks receiving the count (at least one)
    /// @param policy      how a merged derived-delivery failure reflects on the input batch
    /// @return the span-count sink
    public static TracesSink spanCount(List<? extends Sink<? super MetricsData>> downstreams, FailurePolicy policy) {
        return new SpanCountConnector(downstreams, policy);
    }

    /// Log-record count sink routing one count to several downstream metric sinks (at least one)
    /// under `policy`, merging with FanOut semantics.
    ///
    /// @param downstreams the metric sinks receiving the count (at least one)
    /// @param policy      how a merged derived-delivery failure reflects on the input batch
    /// @return the log-record-count sink
    public static LogsSink logRecordCount(
            List<? extends Sink<? super MetricsData>> downstreams, FailurePolicy policy) {
        return new LogRecordCountConnector(downstreams, policy);
    }
}
