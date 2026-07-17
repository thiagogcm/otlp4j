package dev.nthings.otlp4j.service;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.LOGS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static dev.nthings.otlp4j.service.validation.DiagnosticCode.CONNECTOR_PAIR_UNSUPPORTED;
import static dev.nthings.otlp4j.service.validation.DiagnosticCode.CONNECTOR_USED_AS_EXPORTER_ONLY;
import static dev.nthings.otlp4j.service.validation.DiagnosticCode.CONNECTOR_USED_AS_RECEIVER_ONLY;
import static dev.nthings.otlp4j.service.validation.DiagnosticCode.CYCLE_DETECTED;
import static dev.nthings.otlp4j.service.validation.DiagnosticCode.DUPLICATE_PROCESSOR_IN_PIPELINE;
import static dev.nthings.otlp4j.service.validation.DiagnosticCode.EMPTY_PIPELINE;
import static dev.nthings.otlp4j.service.validation.DiagnosticCode.EXPORTER_SIGNAL_UNSUPPORTED;
import static dev.nthings.otlp4j.service.validation.DiagnosticCode.ORPHANED_COMPONENT;
import static dev.nthings.otlp4j.service.validation.DiagnosticCode.PIPELINE_NO_EXPORTERS;
import static dev.nthings.otlp4j.service.validation.DiagnosticCode.PIPELINE_NO_RECEIVERS;
import static dev.nthings.otlp4j.service.validation.DiagnosticCode.PROCESSOR_SIGNAL_UNSUPPORTED;
import static dev.nthings.otlp4j.service.validation.DiagnosticCode.RECEIVER_SIGNAL_UNSUPPORTED;
import static dev.nthings.otlp4j.service.validation.DiagnosticCode.UNKNOWN_CONNECTOR_REF;
import static dev.nthings.otlp4j.service.validation.DiagnosticCode.UNKNOWN_EXPORTER_REF;
import static dev.nthings.otlp4j.service.validation.DiagnosticCode.UNKNOWN_PROCESSOR_REF;
import static dev.nthings.otlp4j.service.validation.DiagnosticCode.UNKNOWN_RECEIVER_REF;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.signal.SignalPair;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import dev.nthings.otlp4j.service.validation.Diagnostic;
import dev.nthings.otlp4j.service.validation.DiagnosticCode;
import dev.nthings.otlp4j.service.validation.GraphValidationException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/// One test per [DiagnosticCode]: the enumerated fixture matrix that drives every validation branch.
/// Each fixture is built so the target code is present in the reported diagnostics; some minimal
/// fixtures also surface a knock-on orphan, which is expected and does not weaken the assertion that
/// the target code was produced.
class DiagnosticsTest {

    private static final ComponentId R = ComponentId.parse("otlp");
    private static final ComponentId E = ComponentId.parse("otlpexp");
    private static final ComponentId P = ComponentId.parse("batch");
    private static final ComponentId C = ComponentId.parse("spanmetrics");
    private static final ComponentId C2 = ComponentId.parse("metricstraces");

    private static final PipelineId TRACES_PIPE = PipelineId.of(TRACES);
    private static final PipelineId METRICS_PIPE = PipelineId.of(METRICS);
    private static final PipelineId LOGS_PIPE = PipelineId.of(LOGS);

    private final Fakes.Log log = new Fakes.Log();

    private Fakes.ReceiverFactoryImpl receiver(Set<dev.nthings.otlp4j.core.signal.SignalDescriptor<?>> supported) {
        return new Fakes.ReceiverFactoryImpl("otlp", log, supported);
    }

    private Fakes.ExporterFactoryImpl exporter(Set<dev.nthings.otlp4j.core.signal.SignalDescriptor<?>> supported) {
        return new Fakes.ExporterFactoryImpl("otlpexp", log, supported, null);
    }

    private Fakes.ProcessorFactoryImpl processor(Set<dev.nthings.otlp4j.core.signal.SignalDescriptor<?>> supported) {
        return new Fakes.ProcessorFactoryImpl("batch", log, supported);
    }

    private Fakes.ConnectorFactoryImpl connector(String type, Set<SignalPair<?, ?>> pairs) {
        return new Fakes.ConnectorFactoryImpl(type, log, pairs);
    }

    private static Set<DiagnosticCode> codes(GraphValidationException ex) {
        return ex.diagnostics().stream().map(Diagnostic::code).collect(Collectors.toSet());
    }

    private GraphValidationException validating(GatewayAssembler assembler) {
        return catchThrowableOfType(GraphValidationException.class, assembler::build);
    }

    @Test
    void unknownReceiverRef() {
        var ex = validating(Gateway.builder()
                .exporter(E, exporter(null), Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(ComponentId.parse("missing")), List.of(), List.of(E))));
        assertThat(codes(ex)).contains(UNKNOWN_RECEIVER_REF);
    }

    @Test
    void unknownProcessorRef() {
        var ex = validating(Gateway.builder()
                .receiver(R, receiver(null), Fakes.Config.ok())
                .exporter(E, exporter(null), Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(ComponentId.parse("missing")), List.of(E))));
        assertThat(codes(ex)).contains(UNKNOWN_PROCESSOR_REF);
    }

    @Test
    void unknownExporterRef() {
        var ex = validating(Gateway.builder()
                .receiver(R, receiver(null), Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(), List.of(ComponentId.parse("missing")))));
        assertThat(codes(ex)).contains(UNKNOWN_EXPORTER_REF);
    }

    @Test
    void unknownConnectorRef() {
        var bridge = ComponentId.parse("bridge");
        var ex = validating(Gateway.builder()
                .receiver(R, receiver(null), Fakes.Config.ok())
                .exporter(E, exporter(null), Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(), List.of(bridge)))
                .pipeline(METRICS_PIPE, new PipelineConfig(List.of(bridge), List.of(), List.of(E))));
        assertThat(codes(ex)).contains(UNKNOWN_CONNECTOR_REF);
    }

    @Test
    void emptyPipeline() {
        var ex = validating(Gateway.builder()
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(), List.of(), List.of())));
        assertThat(codes(ex)).contains(EMPTY_PIPELINE);
    }

    @Test
    void pipelineNoReceivers() {
        var ex = validating(Gateway.builder()
                .processor(P, processor(null), Fakes.Config.ok())
                .exporter(E, exporter(null), Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(), List.of(P), List.of(E))));
        assertThat(codes(ex)).contains(PIPELINE_NO_RECEIVERS);
    }

    @Test
    void pipelineNoExporters() {
        var ex = validating(Gateway.builder()
                .receiver(R, receiver(null), Fakes.Config.ok())
                .processor(P, processor(null), Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(P), List.of())));
        assertThat(codes(ex)).contains(PIPELINE_NO_EXPORTERS);
    }

    @Test
    void receiverSignalUnsupported() {
        var ex = validating(Gateway.builder()
                .receiver(R, receiver(Fakes.signals(METRICS)), Fakes.Config.ok())
                .exporter(E, exporter(null), Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(), List.of(E))));
        assertThat(codes(ex)).contains(RECEIVER_SIGNAL_UNSUPPORTED);
    }

    @Test
    void processorSignalUnsupported() {
        var ex = validating(Gateway.builder()
                .receiver(R, receiver(null), Fakes.Config.ok())
                .processor(P, processor(Fakes.signals(METRICS)), Fakes.Config.ok())
                .exporter(E, exporter(null), Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(P), List.of(E))));
        assertThat(codes(ex)).contains(PROCESSOR_SIGNAL_UNSUPPORTED);
    }

    @Test
    void exporterSignalUnsupported() {
        var ex = validating(Gateway.builder()
                .receiver(R, receiver(null), Fakes.Config.ok())
                .exporter(E, exporter(Fakes.signals(METRICS)), Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(), List.of(E))));
        assertThat(codes(ex)).contains(EXPORTER_SIGNAL_UNSUPPORTED);
    }

    @Test
    void connectorPairUnsupported() {
        var eLogs = ComponentId.parse("logsexp");
        var ex = validating(Gateway.builder()
                .receiver(R, receiver(null), Fakes.Config.ok())
                .exporter(eLogs, exporter(null), Fakes.Config.ok())
                .connector(C, connector("spanmetrics", Fakes.pairs(new SignalPair<>(TRACES, METRICS))), Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(), List.of(C)))
                .pipeline(LOGS_PIPE, new PipelineConfig(List.of(C), List.of(), List.of(eLogs))));
        assertThat(codes(ex)).contains(CONNECTOR_PAIR_UNSUPPORTED);
    }

    @Test
    void connectorUsedAsExporterOnly() {
        var ex = validating(Gateway.builder()
                .receiver(R, receiver(null), Fakes.Config.ok())
                .connector(C, connector("spanmetrics", Fakes.pairs(new SignalPair<>(TRACES, METRICS))), Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(), List.of(C))));
        assertThat(codes(ex)).contains(CONNECTOR_USED_AS_EXPORTER_ONLY);
    }

    @Test
    void connectorUsedAsReceiverOnly() {
        var ex = validating(Gateway.builder()
                .exporter(E, exporter(null), Fakes.Config.ok())
                .connector(C, connector("spanmetrics", Fakes.pairs(new SignalPair<>(TRACES, METRICS))), Fakes.Config.ok())
                .pipeline(METRICS_PIPE, new PipelineConfig(List.of(C), List.of(), List.of(E))));
        assertThat(codes(ex)).contains(CONNECTOR_USED_AS_RECEIVER_ONLY);
    }

    @Test
    void duplicateProcessorInPipeline() {
        var ex = validating(Gateway.builder()
                .receiver(R, receiver(null), Fakes.Config.ok())
                .processor(P, processor(null), Fakes.Config.ok())
                .exporter(E, exporter(null), Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(P, P), List.of(E))));
        assertThat(codes(ex)).contains(DUPLICATE_PROCESSOR_IN_PIPELINE);
    }

    @Test
    void cycleDetected() {
        var ex = validating(Gateway.builder()
                .receiver(R, receiver(null), Fakes.Config.ok())
                .connector(C, connector("spanmetrics", Fakes.pairs(new SignalPair<>(TRACES, METRICS))), Fakes.Config.ok())
                .connector(C2, connector("metricstraces", Fakes.pairs(new SignalPair<>(METRICS, TRACES))), Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R, C2), List.of(), List.of(C)))
                .pipeline(METRICS_PIPE, new PipelineConfig(List.of(C), List.of(), List.of(C2))));
        assertThat(codes(ex)).contains(CYCLE_DETECTED);
    }

    @Test
    void orphanedComponent() {
        var e2 = ComponentId.parse("otlpexp2");
        var ex = validating(Gateway.builder()
                .receiver(R, receiver(null), Fakes.Config.ok())
                .exporter(E, exporter(null), Fakes.Config.ok())
                .exporter(e2, exporter(null), Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(), List.of(E))));
        assertThat(codes(ex)).contains(ORPHANED_COMPONENT);
    }

    @Test
    void validConfigDoesNotThrow() {
        // A well-formed traces pipeline validates and builds without any diagnostic.
        var runtime = Gateway.builder()
                .receiver(R, receiver(null), Fakes.Config.ok())
                .exporter(E, exporter(null), Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(), List.of(E)))
                .build();
        assertThat(runtime).isNotNull();
        runtime.close();
    }
}
