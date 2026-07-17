package dev.nthings.otlp4j.service;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import dev.nthings.otlp4j.service.config.ServiceOptions;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/// Proves the graph-derived lifecycle order: reverse-topological instantiation and start,
/// forward-topological shutdown, and rollback in reverse start order when a start fails. The topology
/// is the linear chain `otlp -> batch/1 -> batch/2 -> otlpexp`, so every order is unique.
class LifecycleOrderTest {

    private static final ComponentId R = ComponentId.parse("otlp");
    private static final ComponentId P1 = ComponentId.parse("batch/1");
    private static final ComponentId P2 = ComponentId.parse("batch/2");
    private static final ComponentId E = ComponentId.parse("otlpexp");
    private static final PipelineId TRACES_PIPE = PipelineId.of(TRACES);

    private final Fakes.Log log = new Fakes.Log();

    private GatewayBuilder chain(boolean failMiddleProcessor) {
        var recv = new Fakes.ReceiverFactoryImpl("otlp", log, null);
        var proc = new Fakes.ProcessorFactoryImpl("batch", log, null);
        var exp = new Fakes.ExporterFactoryImpl("otlpexp", log, null, null);
        var gw = Gateway.builder();
        gw.receiver(R, recv, Fakes.Config.ok());
        gw.processor(P1, proc, failMiddleProcessor ? Fakes.Config.failingStart() : Fakes.Config.ok());
        gw.processor(P2, proc, Fakes.Config.ok());
        gw.exporter(E, exp, Fakes.Config.ok());
        gw.service(new ServiceOptions(Duration.ofSeconds(5), Duration.ofSeconds(5)));
        gw.pipeline(TRACES_PIPE, new PipelineConfig(List.of(R), List.of(P1, P2), List.of(E)));
        return gw;
    }

    private List<String> filtered(String prefix) {
        return log.events().stream()
                .filter(e -> e.startsWith(prefix))
                .map(e -> e.substring(prefix.length()))
                .collect(Collectors.toList());
    }

    @Test
    void instantiatesInReverseTopologicalOrder() {
        chain(false).build();
        assertThat(filtered("create:")).containsSubsequence("otlpexp", "batch/2", "batch/1", "otlp");
    }

    @Test
    void startsInReverseTopologicalOrder() {
        var runtime = chain(false).build();
        runtime.start();
        assertThat(filtered("start:")).containsExactly("otlpexp", "batch/2", "batch/1", "otlp");
        runtime.shutdown(Duration.ofSeconds(5));
    }

    @Test
    void shutsDownForwardTopological() {
        var runtime = chain(false).build();
        runtime.start();
        runtime.shutdown(Duration.ofSeconds(5));
        assertThat(filtered("shutdown:")).containsExactly("otlp", "batch/1", "batch/2", "otlpexp");
    }

    @Test
    void rollsBackAlreadyStartedInReverseStartOrderOnStartFailure() {
        var runtime = chain(true).build();

        var failure = catchThrowableOfType(GatewayStartException.class, runtime::start);

        assertThat(failure).isNotNull();
        assertThat(failure.component()).isEqualTo(P1);
        // otlpexp, batch/2 started; batch/1 failed; otlp never started.
        assertThat(filtered("start:")).containsExactly("otlpexp", "batch/2", "batch/1");
        // rollback shuts down the started components in reverse start order; the failed and unstarted
        // components are not shut down.
        assertThat(filtered("shutdown:")).containsExactly("batch/2", "otlpexp");
    }
}
