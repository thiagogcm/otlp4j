package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.codec.Payloads;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.NetworkBound;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.consumer.SignalConsumers;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Disposition;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.SignalPair;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/// End-to-end behaviour of a built runtime: reading a graph-booted receiver's bound port through the
/// runtime component lookup, the graph dump, a best-effort flush, fan-out delivery with result
/// merging, and a connector pipeline that builds, starts, and stops.
class RuntimeBehaviourTest {

    private static final ComponentId R = ComponentId.parse("otlp");
    private static final ComponentId E1 = ComponentId.parse("otlpexp/a");
    private static final ComponentId E2 = ComponentId.parse("otlpexp/b");
    private static final PipelineId TRACES_PIPE = PipelineId.of(TRACES);

    private final Fakes.Log log = new Fakes.Log();

    private final TelemetryPayload payload =
            Payloads.domain(TRACES, new TracesData(List.of()));

    /// The core `SignalConsumers.consumer` is keyed by the payload type (`TelemetryPayload`) while the
    /// public `SignalDescriptor` constants are keyed by the model batch type (`TracesData`), so the
    /// constant cannot be handed to `consumer` without a cast. This mirrors the cast the runtime and
    /// the receiver adapter make internally.
    @SuppressWarnings("unchecked")
    private static SignalConsumer<TelemetryPayload> tracesConsumer(SignalConsumers bundle) {
        return (SignalConsumer<TelemetryPayload>) bundle.consumer((SignalDescriptor<TelemetryPayload>) (SignalDescriptor<?>) TRACES);
    }

    private GatewayAssembler fanOutGateway(DeliveryResult secondOutcome) {
        var recv = new Fakes.ReceiverFactoryImpl("otlp", log, null);
        var exp1 = new Fakes.ExporterFactoryImpl("otlpexp", log, null, null);
        var exp2 = new Fakes.ExporterFactoryImpl("otlpexp", log, null, secondOutcome);
        return Gateway.builder()
                .receiver(R, recv, Fakes.Config.ok())
                .exporter(E1, exp1, Fakes.Config.ok())
                .exporter(E2, exp2, Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(), List.of(E1, E2)));
    }

    @Test
    void exposesBoundPortThroughComponentLookup() {
        var runtime = fanOutGateway(null).build();
        runtime.start();

        var port = runtime.component(R)
                .filter(NetworkBound.class::isInstance)
                .map(NetworkBound.class::cast)
                .map(NetworkBound::port)
                .orElseThrow();
        assertThat(port).isEqualTo(45678);

        runtime.shutdown(Duration.ofSeconds(5));
    }

    @Test
    void describeRendersNodesAndEdges() {
        var runtime = fanOutGateway(null).build();
        var dump = runtime.describe();
        assertThat(dump).contains("digraph gateway");
        assertThat(dump).contains("receiver otlp");
        assertThat(dump).contains("exporter otlpexp/a");
        assertThat(dump).contains("fanout");
    }

    @Test
    void forceFlushIsBestEffort() {
        var runtime = fanOutGateway(null).build();
        runtime.start();
        runtime.forceFlush(Duration.ofSeconds(1));
        runtime.close();
    }

    @Test
    void fanOutDeliversToAllPeersAndMergesAccepted() {
        var runtime = fanOutGateway(null).build();
        runtime.start();

        var receiver = (Fakes.ReceiverImpl) runtime.component(R).orElseThrow();
        var result = tracesConsumer(receiver.consumers())
                .consume(new TestConsumeContext(), payload)
                .toCompletableFuture()
                .join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        // Both exporters were delivered to.
        assertThat(log.events()).contains("consume:otlpexp/a", "consume:otlpexp/b");
        runtime.shutdown(Duration.ofSeconds(5));
    }

    @Test
    void fanOutMergePropagatesAPeerRejection() {
        var rejection = DeliveryResult.permanent(
                "downstream refused", new Provenance(E2, ComponentKind.EXPORTER, TRACES));
        var runtime = fanOutGateway(rejection).build();
        runtime.start();

        var receiver = (Fakes.ReceiverImpl) runtime.component(R).orElseThrow();
        var result = tracesConsumer(receiver.consumers())
                .consume(new TestConsumeContext(), payload)
                .toCompletableFuture()
                .join();

        assertThat(result).isInstanceOf(DeliveryResult.Rejected.class);
        assertThat(((DeliveryResult.Rejected) result).disposition()).isEqualTo(Disposition.PERMANENT);
        runtime.shutdown(Duration.ofSeconds(5));
    }

    @Test
    void fanOutWithMutatingPeerBuildsAndDelivers() {
        var p = ComponentId.parse("redact");
        var eSecond = ComponentId.parse("otlpexp/second");
        var recv = new Fakes.ReceiverFactoryImpl("otlp", log, null);
        var mutatingProc = new Fakes.ProcessorFactoryImpl("redact", log, null, true);
        var exp1 = new Fakes.ExporterFactoryImpl("otlpexp", log, null, null);
        var exp2 = new Fakes.ExporterFactoryImpl("otlpexp", log, null, null);

        // The receiver feeds two same-signal pipelines, so it fans out to a mutating processor branch
        // and a plain exporter branch, exercising the copy-before-mutating-peer seam.
        var runtime = Gateway.builder()
                .receiver(R, recv, Fakes.Config.ok())
                .processor(p, mutatingProc, Fakes.Config.ok())
                .exporter(E1, exp1, Fakes.Config.ok())
                .exporter(eSecond, exp2, Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(p), List.of(E1)))
                .pipeline(PipelineId.of(TRACES, "second"), new PipelineConfig(List.of(R), List.of(), List.of(eSecond)))
                .build();
        runtime.start();

        var receiver = (Fakes.ReceiverImpl) runtime.component(R).orElseThrow();
        var result = tracesConsumer(receiver.consumers())
                .consume(new TestConsumeContext(), payload)
                .toCompletableFuture()
                .join();

        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(log.events()).contains("consume:redact", "consume:otlpexp/a", "consume:otlpexp/second");
        runtime.close();
    }

    @Test
    void connectorPipelineBuildsStartsAndStops() {
        var eMetrics = ComponentId.parse("metricsexp");
        var connectorId = ComponentId.parse("spanmetrics");
        var recv = new Fakes.ReceiverFactoryImpl("otlp", log, null);
        var exp = new Fakes.ExporterFactoryImpl("metricsexp", log, null, null);
        var connector = new Fakes.ConnectorFactoryImpl(
                "spanmetrics", log, Fakes.pairs(new SignalPair<>(TRACES, METRICS)));

        var runtime = Gateway.builder()
                .receiver(R, recv, Fakes.Config.ok())
                .exporter(eMetrics, exp, Fakes.Config.ok())
                .connector(connectorId, connector, Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(), List.of(connectorId)))
                .pipeline(PipelineId.of(METRICS), new PipelineConfig(List.of(connectorId), List.of(), List.of(eMetrics)))
                .build();
        runtime.start();

        assertThat(runtime.component(connectorId)).isPresent();
        assertThat(log.events()).contains("start:spanmetrics", "start:metricsexp", "start:otlp");

        runtime.shutdown(Duration.ofSeconds(5));
        assertThat(log.events()).contains("shutdown:otlp", "shutdown:spanmetrics", "shutdown:metricsexp");
    }
}
