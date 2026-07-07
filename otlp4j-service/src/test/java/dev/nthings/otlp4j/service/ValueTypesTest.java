package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.signal.ModelBatchPayload;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.signal.SignalPair;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.service.config.ComponentRegistry;
import dev.nthings.otlp4j.service.config.ConnectorEntry;
import dev.nthings.otlp4j.service.config.ExporterEntry;
import dev.nthings.otlp4j.service.config.ExtensionEntry;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import dev.nthings.otlp4j.service.config.ProcessorEntry;
import dev.nthings.otlp4j.service.config.ReceiverEntry;
import dev.nthings.otlp4j.service.config.ServiceOptions;
import dev.nthings.otlp4j.service.validation.Diagnostic;
import dev.nthings.otlp4j.service.validation.DiagnosticCode;
import dev.nthings.otlp4j.service.validation.GraphValidationException;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/// Value-type and config coverage: entry accessors and validation, the component registry lookups,
/// the config records' invariants, the payload's invariants, and the diagnostic types.
class ValueTypesTest {

    private static final ComponentId ID = ComponentId.parse("x");
    private final Fakes.Log log = new Fakes.Log();

    @Test
    void entriesExposeKindTypeAndValidate() {
        var recv = new ReceiverEntry<>(new Fakes.ReceiverFactoryImpl("otlp", log, null), Fakes.Config.ok());
        assertThat(recv.kind()).isEqualTo(ComponentKind.RECEIVER);
        assertThat(recv.type()).isEqualTo("otlp");

        var proc = new ProcessorEntry<>(new Fakes.ProcessorFactoryImpl("batch", log, null), Fakes.Config.ok());
        assertThat(proc.kind()).isEqualTo(ComponentKind.PROCESSOR);
        assertThat(proc.type()).isEqualTo("batch");

        var exp = new ExporterEntry<>(new Fakes.ExporterFactoryImpl("otlpexp", log, null, null), Fakes.Config.ok());
        assertThat(exp.kind()).isEqualTo(ComponentKind.EXPORTER);
        assertThat(exp.type()).isEqualTo("otlpexp");

        var pairs = Fakes.pairs(new SignalPair<>(TRACES, METRICS));
        var conn = new ConnectorEntry<>(new Fakes.ConnectorFactoryImpl("spanmetrics", log, pairs), Fakes.Config.ok());
        assertThat(conn.kind()).isEqualTo(ComponentKind.CONNECTOR);
        assertThat(conn.type()).isEqualTo("spanmetrics");
        assertThat(conn.supportedPairs()).isEqualTo(pairs);

        var ext = new ExtensionEntry<>(new Fakes.ExtensionFactoryImpl("health", log), Fakes.Config.ok());
        assertThat(ext.kind()).isEqualTo(ComponentKind.EXTENSION);
        assertThat(ext.type()).isEqualTo("health");
    }

    @Test
    void entriesRejectNulls() {
        var rf = new Fakes.ReceiverFactoryImpl("otlp", log, null);
        assertThatThrownBy(() -> new ReceiverEntry<>(null, Fakes.Config.ok())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ReceiverEntry<>(rf, null)).isInstanceOf(IllegalArgumentException.class);
        var pf = new Fakes.ProcessorFactoryImpl("batch", log, null);
        assertThatThrownBy(() -> new ProcessorEntry<>(null, Fakes.Config.ok())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ProcessorEntry<>(pf, null)).isInstanceOf(IllegalArgumentException.class);
        var ef = new Fakes.ExporterFactoryImpl("e", log, null, null);
        assertThatThrownBy(() -> new ExporterEntry<>(null, Fakes.Config.ok())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ExporterEntry<>(ef, null)).isInstanceOf(IllegalArgumentException.class);
        var cf = new Fakes.ConnectorFactoryImpl("c", log, Fakes.pairs());
        assertThatThrownBy(() -> new ConnectorEntry<>(null, Fakes.Config.ok())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ConnectorEntry<>(cf, null)).isInstanceOf(IllegalArgumentException.class);
        var xf = new Fakes.ExtensionFactoryImpl("h", log);
        assertThatThrownBy(() -> new ExtensionEntry<>(null, Fakes.Config.ok())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ExtensionEntry<>(xf, null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void componentRegistryAnswersLookupsAndSignalSupport() {
        var config = Gateway.builder()
                .receiver(ComponentId.parse("otlp"), new Fakes.ReceiverFactoryImpl("otlp", log, Fakes.signals(TRACES)), Fakes.Config.ok())
                .processor(ComponentId.parse("batch"), new Fakes.ProcessorFactoryImpl("batch", log, null), Fakes.Config.ok())
                .exporter(ComponentId.parse("otlpexp"), new Fakes.ExporterFactoryImpl("otlpexp", log, null, null), Fakes.Config.ok())
                .connector(ComponentId.parse("c"), new Fakes.ConnectorFactoryImpl("c", log, Fakes.pairs()), Fakes.Config.ok())
                .extension(ComponentId.parse("h"), new Fakes.ExtensionFactoryImpl("h", log), Fakes.Config.ok())
                .toConfig();
        var registry = new ComponentRegistry(config);

        assertThat(registry.config()).isSameAs(config);
        assertThat(registry.hasReceiver(ComponentId.parse("otlp"))).isTrue();
        assertThat(registry.hasProcessor(ComponentId.parse("batch"))).isTrue();
        assertThat(registry.hasExporter(ComponentId.parse("otlpexp"))).isTrue();
        assertThat(registry.hasConnector(ComponentId.parse("c"))).isTrue();
        assertThat(registry.hasExtension(ComponentId.parse("h"))).isTrue();
        assertThat(registry.receiver(ComponentId.parse("otlp"))).isNotNull();
        assertThat(registry.processor(ComponentId.parse("batch"))).isNotNull();
        assertThat(registry.exporter(ComponentId.parse("otlpexp"))).isNotNull();
        assertThat(registry.connector(ComponentId.parse("c"))).isNotNull();
        assertThat(registry.extension(ComponentId.parse("h"))).isNotNull();

        // SignalSupport restriction is honored.
        assertThat(registry.receiverSupports(ComponentId.parse("otlp"), TRACES)).isTrue();
        assertThat(registry.receiverSupports(ComponentId.parse("otlp"), METRICS)).isFalse();
        // A processor with no restriction supports any signal.
        assertThat(registry.processorSupports(ComponentId.parse("batch"), METRICS)).isTrue();
        // An unknown id resolves to no factory, so it defaults to supported.
        assertThat(registry.exporterSupports(ComponentId.parse("missing"), TRACES)).isTrue();

        assertThatThrownBy(() -> new ComponentRegistry(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void pipelineConfigCopiesAndValidates() {
        var pc = PipelineConfig.of(List.of(ID), List.of(ID));
        assertThat(pc.processors()).isEmpty();
        assertThatThrownBy(() -> new PipelineConfig(null, List.of(), List.of())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void serviceOptionsValidateDeadlines() {
        assertThat(ServiceOptions.defaults().extensions()).isEmpty();
        assertThatThrownBy(() -> new ServiceOptions(Duration.ZERO, Duration.ofSeconds(1), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ServiceOptions(Duration.ofSeconds(1), Duration.ofSeconds(-1), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ServiceOptions(Duration.ofSeconds(1), Duration.ofSeconds(1), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void modelBatchPayloadRejectsNulls() {
        assertThatThrownBy(() -> new ModelBatchPayload<>(null, new TracesData(List.of())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ModelBatchPayload<>(TRACES, null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void diagnosticTypesRenderAndValidate() {
        var pid = PipelineId.of(TRACES);
        var d = Diagnostic.of(DiagnosticCode.CYCLE_DETECTED, "loop", ID, pid);
        assertThat(d.toString()).contains("CYCLE_DETECTED").contains("component=x").contains("pipeline=traces");
        assertThat(Diagnostic.ofPipeline(DiagnosticCode.EMPTY_PIPELINE, "empty", pid).component()).isEmpty();
        assertThatThrownBy(() -> new Diagnostic(null, "m", java.util.Optional.empty(), java.util.Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Diagnostic(DiagnosticCode.EMPTY_PIPELINE, null, java.util.Optional.empty(), java.util.Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);

        var ex = new GraphValidationException(List.of(d));
        assertThat(ex.diagnostics()).containsExactly(d);
        assertThat(ex.getMessage()).contains("CYCLE_DETECTED");
        assertThatThrownBy(() -> new GraphValidationException(List.of())).isInstanceOf(IllegalArgumentException.class);
    }
}
