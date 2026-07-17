package dev.nthings.otlp4j.service;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import dev.nthings.otlp4j.service.config.ServiceOptions;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/// The runtime's error and edge paths: a failing shutdown rethrown after best-effort completion, a
/// failing flush, a start timeout that triggers rollback, the swallow-on-close behaviour, and the
/// empty component lookup.
class RuntimeErrorPathsTest {

    private static final ComponentId R = ComponentId.parse("otlp");
    private static final ComponentId E = ComponentId.parse("otlpexp");
    private static final PipelineId TRACES_PIPE = PipelineId.of(TRACES);

    private final Fakes.Log log = new Fakes.Log();

    private GatewayRuntime gateway(Fakes.Config exporterConfig, ServiceOptions options) {
        return Gateway.builder()
                .receiver(R, new Fakes.ReceiverFactoryImpl("otlp", log, null), Fakes.Config.ok())
                .exporter(E, new Fakes.ExporterFactoryImpl("otlpexp", log, null, null), exporterConfig)
                .service(options)
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(), List.of(E)))
                .build();
    }

    private static ServiceOptions options(Duration start) {
        return new ServiceOptions(start, Duration.ofSeconds(5));
    }

    @Test
    void shutdownIsBestEffortButRethrowsFirstFailure() {
        var runtime = gateway(Fakes.Config.failingShutdown(), options(Duration.ofSeconds(5)));
        runtime.start();

        assertThatThrownBy(() -> runtime.shutdown(Duration.ofSeconds(5))).isInstanceOf(RuntimeException.class);
        // Both components were still asked to shut down despite the exporter failing.
        assertThat(log.events()).contains("shutdown:otlp", "shutdown:otlpexp");
    }

    @Test
    void forceFlushRethrowsFirstFailure() {
        var runtime = gateway(Fakes.Config.failingFlush(), options(Duration.ofSeconds(5)));
        runtime.start();

        assertThatThrownBy(() -> runtime.forceFlush(Duration.ofSeconds(1))).isInstanceOf(RuntimeException.class);
        runtime.close();
    }

    @Test
    void startTimeoutRollsBack() {
        // The receiver hangs on start; it starts last, so the exporter is already up and rolled back.
        var runtime = Gateway.builder()
                .receiver(R, new Fakes.ReceiverFactoryImpl("otlp", log, null), Fakes.Config.hangingStart())
                .exporter(E, new Fakes.ExporterFactoryImpl("otlpexp", log, null, null), Fakes.Config.ok())
                .service(options(Duration.ofMillis(100)))
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(), List.of(E)))
                .build();

        var failure = catchThrowableOfType(GatewayStartException.class, runtime::start);
        assertThat(failure).isNotNull();
        assertThat(failure.component()).isEqualTo(R);
        // The exporter that had started is rolled back.
        assertThat(log.events()).contains("shutdown:otlpexp");
    }

    @Test
    void closeSwallowsShutdownFailure() {
        var runtime = gateway(Fakes.Config.failingShutdown(), options(Duration.ofSeconds(5)));
        runtime.start();
        // close() must not throw even though the exporter's shutdown fails.
        runtime.close();
        assertThat(log.events()).contains("shutdown:otlpexp");
    }

    @Test
    void componentLookupIsEmptyForUnknownId() {
        var runtime = gateway(Fakes.Config.ok(), options(Duration.ofSeconds(5)));
        assertThat(runtime.component(ComponentId.parse("nope"))).isEmpty();
        assertThat(runtime.component(R)).isPresent();
    }

    @Test
    void gatewayBuilderRejectsNullConfigAndContext() {
        assertThatThrownBy(() -> new GatewayBuilder(null)).isInstanceOf(IllegalArgumentException.class);
        var builder = new GatewayBuilder(gatewayConfig());
        assertThatThrownBy(() -> builder.build(null)).isInstanceOf(IllegalArgumentException.class);
    }

    private dev.nthings.otlp4j.service.config.GatewayConfig gatewayConfig() {
        return Gateway.builder()
                .receiver(R, new Fakes.ReceiverFactoryImpl("otlp", log, null), Fakes.Config.ok())
                .exporter(E, new Fakes.ExporterFactoryImpl("otlpexp", log, null, null), Fakes.Config.ok())
                .pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(), List.of(E)))
                .toConfig();
    }
}
