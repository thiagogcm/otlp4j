package dev.nthings.otlp4j.samples.support;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.Connector;
import dev.nthings.otlp4j.core.component.ConnectorFactory;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.codec.Payloads;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.SignalPair;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.model.Resource;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

/// A demo seed [ConnectorFactory] that bridges the traces-to-metrics pair, deriving a single
/// delta-sum count metric from each traces batch and emitting it into the receiving metrics pipeline.
/// This reproduces the legacy span-count connector without the full deriving-connector helper, which
/// is a later-phase component.
public final class CountConnectorFactory implements ConnectorFactory<CountConnectorConfig> {

    private static final Set<SignalPair<?, ?>> PAIRS = Set.of(new SignalPair<>(TRACES, METRICS));

    private static final InstrumentationScope SCOPE =
            InstrumentationScope.of("otlp4j-count-connector", "0.1.0");

    /// The component type name for the count connector.
    public static final String TYPE = "spancount";

    @Override
    public ComponentKind kind() {
        return ComponentKind.CONNECTOR;
    }

    @Override
    public CountConnectorConfig defaultConfig() {
        return CountConnectorConfig.defaults();
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Set<SignalPair<?, ?>> supportedPairs() {
        return PAIRS;
    }

    @Override
    public Connector create(
            CreateContext ctx, CountConnectorConfig config, SignalPair<?, ?> pair, SignalConsumer<?> next) {
        return new CountConnector(ctx.id(), config, ProcessorFactory.asPayloadConsumer(next));
    }

    /// The connector component. It consumes a traces payload, counts its items through the payload's
    /// own item count, builds a single-point delta-sum metric over the window since the last count,
    /// and forwards it to the receiving metrics pipeline. Its result is the downstream metrics
    /// pipeline's result, so a metric that the downstream rejects surfaces to the traces side.
    private static final class CountConnector implements Connector, SignalConsumer<TelemetryPayload> {

        private final CountConnectorConfig config;

        private final SignalConsumer<TelemetryPayload> next;

        private final AtomicLong previousFlushNanos = new AtomicLong(nowEpochNanos());

        CountConnector(ComponentId id, CountConnectorConfig config, SignalConsumer<TelemetryPayload> next) {
            this.config = config;
            this.next = next;
        }

        @Override
        public ConsumerCapabilities capabilities() {
            return ConsumerCapabilities.nonMutating();
        }

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext context, TelemetryPayload data) {
            var count = data.itemCount();
            var metrics = deltaSum(count);
            return next.consume(context, Payloads.domain(METRICS, metrics));
        }

        /// A single-point DELTA sum over the window since the previous count, carrying `count`.
        private MetricsData deltaSum(long count) {
            var now = nowEpochNanos();
            var start = previousFlushNanos.getAndAccumulate(now, Math::max);
            var end = Math.max(start, now);
            var point = new NumberPoint(
                    Attributes.empty(), start, end, NumberPoint.longValue(count), 0L, List.of());
            var metric = new Metric(
                    config.metricName(),
                    config.description(),
                    "{item}",
                    new Metric.Sum(List.of(point), Metric.AggregationTemporality.DELTA, true),
                    Attributes.empty());
            return MetricsData.of(Resource.EMPTY, SCOPE, List.of(metric));
        }

        @Override
        public CompletionStage<Void> start(ComponentContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext context) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /// Nanosecond wall clock (`currentTimeMillis` is only millisecond-resolution).
    private static long nowEpochNanos() {
        var now = Instant.now();
        return now.getEpochSecond() * 1_000_000_000L + now.getNano();
    }
}
