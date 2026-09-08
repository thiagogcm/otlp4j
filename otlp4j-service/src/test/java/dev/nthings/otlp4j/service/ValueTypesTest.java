package dev.nthings.otlp4j.service;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.METRICS;
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.signal.SignalPair;
import dev.nthings.otlp4j.service.config.ConnectorEntry;
import dev.nthings.otlp4j.service.config.ExporterEntry;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import dev.nthings.otlp4j.service.config.ProcessorEntry;
import dev.nthings.otlp4j.service.config.ReceiverEntry;
import dev.nthings.otlp4j.service.config.ServiceOptions;
import dev.nthings.otlp4j.service.validation.Diagnostic;
import dev.nthings.otlp4j.service.validation.DiagnosticCode;
import dev.nthings.otlp4j.service.validation.GraphValidationException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/// Value-type and config coverage: entry accessors and validation, the component registry lookups,
/// the config records' invariants and the diagnostic types.
class ValueTypesTest {

    private static final ComponentId ID = ComponentId.parse("x");
    private final Fakes.Log log = new Fakes.Log();

    @Test
    void connectorEntryExposesSupportedPairs() {
        var pairs = Fakes.pairs(new SignalPair<>(TRACES, METRICS));
        var conn = new ConnectorEntry<>(new Fakes.ConnectorFactoryImpl("spanmetrics", log, pairs), Fakes.Config.ok());
        assertThat(conn.supportedPairs()).isEqualTo(pairs);
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
    }

    @Test
    void pipelineConfigCopiesAndValidates() {
        var pc = PipelineConfig.of(List.of(ID), List.of(ID));
        assertThat(pc.processors()).isEmpty();
        assertThatThrownBy(() -> new PipelineConfig(null, List.of(), List.of())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void serviceOptionsValidateDeadlines() {
        assertThat(ServiceOptions.defaults().startDeadline()).isPositive();
        assertThatThrownBy(() -> new ServiceOptions(Duration.ZERO, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ServiceOptions(Duration.ofSeconds(1), Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void diagnosticTypesRenderAndValidate() {
        var pid = PipelineId.of(TRACES);
        var d = Diagnostic.of(DiagnosticCode.CYCLE_DETECTED, "loop", ID, pid);
        assertThat(d.toString()).contains("CYCLE_DETECTED").contains("component=x").contains("pipeline=traces");
        assertThat(Diagnostic.ofPipeline(DiagnosticCode.EMPTY_PIPELINE, "empty", pid).component()).isEmpty();
        assertThatThrownBy(() -> new Diagnostic(null, "m", Optional.empty(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Diagnostic(DiagnosticCode.EMPTY_PIPELINE, null, Optional.empty(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);

        var ex = new GraphValidationException(List.of(d));
        assertThat(ex.diagnostics()).containsExactly(d);
        assertThat(ex.getMessage()).contains("CYCLE_DETECTED");
        assertThatThrownBy(() -> new GraphValidationException(List.of())).isInstanceOf(IllegalArgumentException.class);
    }
}
