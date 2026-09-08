package dev.nthings.otlp4j.components.connector;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.components.support.BatchSupport;
import dev.nthings.otlp4j.components.support.GraphReceiverFactory;
import dev.nthings.otlp4j.components.support.TestConsumeContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.NumberPoint;
import dev.nthings.otlp4j.service.Gateway;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// The promoted span-count connector exercised through a real service-graph runtime: it derives a
/// delta-sum count metric from a traces batch and emits it into a metrics pipeline that an exporter
/// terminates. Migrated from the samples' `SeedFactoriesTest` when the connector moved into
/// `otlp4j-components`.
@DisplayName("Span-count connector")
class SpanCountConnectorFactoryTest {

    private static final ComponentId RECV = ComponentId.parse("in");
    private static final ComponentId CONN = ComponentId.parse("spancount");
    private static final ComponentId METRICS_OUT = ComponentId.parse("metricsout");

    @DisplayName("derives a delta-sum count metric from a traces batch and emits it into the metrics pipeline")
    @Test
    void derivesAndEmitsTheCountMetric() {
        var received = new CopyOnWriteArrayList<Payload<?>>();
        var recv = new GraphReceiverFactory();

        var gw = Gateway.builder();
        var in = gw.receiver(RECV, recv);
        var connector = gw.connector(CONN, new SpanCountConnectorFactory());
        var out = gw.exporter(METRICS_OUT, ExporterFactory.of("capture", (ctx, data, provenance) -> {
            received.add(data);
            return DeliveryResult.acceptedStage(provenance);
        }));
        gw.pipeline(PipelineId.of(TRACES), p -> p.from(in).to(connector));
        gw.pipeline(PipelineId.of(METRICS), p -> p.from(connector).to(out));
        var runtime = gw.build();
        runtime.start();

        var result = recv.deliver(TRACES, new TestConsumeContext(), BatchSupport.traces(3));

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(received).hasSize(1);
        assertThat(received.get(0).signal()).isEqualTo(METRICS);

        var metrics = (MetricsData) received.get(0).data();
        assertThat(metrics.metrics()).singleElement().satisfies(metric -> {
            assertThat(metric.name()).isEqualTo("otlp4j.connector.span.count");
            assertThat(longValueOf(metric)).isEqualTo(3L);
        });
        runtime.close();
    }

    @DisplayName("its default config names the otlp4j.connector.span.count metric")
    @Test
    void defaultConfigNamesTheSpanCountMetric() {
        assertThat(SpanCountConnectorConfig.defaults().metricName()).isEqualTo("otlp4j.connector.span.count");
    }

    private static long longValueOf(Metric metric) {
        if (metric.data() instanceof Metric.Sum sum
                && !sum.points().isEmpty()
                && sum.points().get(0).value() instanceof NumberPoint.LongValue longValue) {
            return longValue.value();
        }
        return -1L;
    }
}
