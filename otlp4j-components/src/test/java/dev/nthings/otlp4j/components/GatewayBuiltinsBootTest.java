package dev.nthings.otlp4j.components;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.components.batch.BatchProcessorConfig;
import dev.nthings.otlp4j.components.batch.BatchProcessorFactory;
import dev.nthings.otlp4j.components.memory.MemoryLimiterProcessorFactory;
import dev.nthings.otlp4j.components.routing.FilterConfig;
import dev.nthings.otlp4j.components.routing.FilterProcessorFactory;
import dev.nthings.otlp4j.components.routing.RoutingConfig;
import dev.nthings.otlp4j.components.routing.RoutingProcessorFactory;
import dev.nthings.otlp4j.components.routing.MatchRule;
import dev.nthings.otlp4j.components.routing.MatchSpec;
import dev.nthings.otlp4j.components.support.GraphReceiverFactory;
import dev.nthings.otlp4j.components.support.TestConsumeContext;
import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.policy.BatchPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.service.Gateway;
import dev.nthings.otlp4j.testing.Fixtures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Boots a real [dev.nthings.otlp4j.service.GatewayRuntime] wiring all four built-in processors -
/// memory limiter, filter, batch, and routing - fed by an in-process receiver and terminated by
/// capturing exporters, and proves data flows through every one of them: the memory limiter admits
/// under the default threshold, the filter keeps only its configured service, the routing branches send
/// each service to the right exporter, and the batch stage holds its data until a clean shutdown flushes
/// it into the still-open downstream.
@DisplayName("Built-in components booted in a gateway")
@Timeout(30)
class GatewayBuiltinsBootTest {

    private static final ComponentId RECV = ComponentId.parse("in");
    private static final ComponentId MEMLIMIT = ComponentId.parse("memlimit");
    private static final ComponentId FILTER = ComponentId.parse("filter");
    private static final ComponentId BATCH = ComponentId.parse("batch");
    private static final ComponentId ROUTE_CHECKOUT = ComponentId.parse("route-checkout");
    private static final ComponentId ROUTE_BILLING = ComponentId.parse("route-billing");
    private static final ComponentId EXP_CHECKOUT = ComponentId.parse("out-checkout");
    private static final ComponentId EXP_BILLING = ComponentId.parse("out-billing");

    @DisplayName("data flows through memory-limiter, filter, batch, and routing; batch flushes on shutdown")
    @Test
    void allFourBuiltInsCarryDataAndBatchDrainsOnShutdown() {
        var checkoutReceived = new CopyOnWriteArrayList<Payload<?>>();
        var billingReceived = new CopyOnWriteArrayList<Payload<?>>();
        var recv = new GraphReceiverFactory();

        var gw = Gateway.builder();
        var in = gw.receiver(RECV, recv);
        var memlimit = gw.processor(MEMLIMIT, new MemoryLimiterProcessorFactory());
        var filter = gw.processor(FILTER, new FilterProcessorFactory(), keepService("checkout"));
        var batch = gw.processor(BATCH, new BatchProcessorFactory(), holdingBatch());
        var routeCheckout = gw.processor(ROUTE_CHECKOUT, new RoutingProcessorFactory(), routeService("checkout"));
        var routeBilling = gw.processor(ROUTE_BILLING, new RoutingProcessorFactory(), routeService("billing"));
        var checkoutOut = gw.exporter(EXP_CHECKOUT, capturingInto(checkoutReceived));
        var billingOut = gw.exporter(EXP_BILLING, capturingInto(billingReceived));

        // The checkout pipeline runs the payload through all four built-ins; the billing pipeline is a
        // second routing branch off the same receiver, proving routing sends each service to its own
        // exporter.
        gw.pipeline(PipelineId.of(TRACES, "checkout"),
                p -> p.from(in).via(memlimit, filter, batch, routeCheckout).to(checkoutOut));
        gw.pipeline(PipelineId.of(TRACES, "billing"),
                p -> p.from(in).via(routeBilling).to(billingOut));
        var runtime = gw.build();
        runtime.start();

        var checkoutResult = recv.deliver(TRACES, new TestConsumeContext(), tracesFor("checkout", 3));
        var billingResult = recv.deliver(TRACES, new TestConsumeContext(), tracesFor("billing", 2));

        // Both admitted: the memory limiter is under its default threshold in the test JVM.
        assertThat(checkoutResult).isInstanceOf(DeliveryResult.Accepted.class);
        assertThat(billingResult).isInstanceOf(DeliveryResult.Accepted.class);

        // Routing sent billing straight to its own exporter; the filter dropped billing from the
        // checkout pipeline, and the batch stage is still holding the checkout payload.
        assertThat(billingReceived).singleElement()
                .satisfies(payload -> assertThat(payload.itemCount()).isEqualTo(2L));
        assertThat(checkoutReceived).as("batch holds the checkout payload until shutdown").isEmpty();

        runtime.close();

        // Shutdown drained the batch into the still-open routing branch and exporter.
        assertThat(checkoutReceived).as("batch flushes pending data on clean shutdown").singleElement()
                .satisfies(payload -> assertThat(payload.itemCount()).isEqualTo(3L));
        assertThat(billingReceived).hasSize(1);
    }

    /// A filter that keeps only payloads whose `service.name` resource attribute equals `service`.
    private static FilterConfig keepService(String service) {
        return new FilterConfig(matchService(service), MatchSpec.none());
    }

    /// A routing branch that forwards only payloads whose `service.name` equals `service`.
    private static RoutingConfig routeService(String service) {
        return new RoutingConfig(matchService(service));
    }

    private static MatchSpec matchService(String service) {
        return MatchSpec.of(MatchRule.builder().resourceAttribute("service.name", service).build());
    }

    /// A batch config that never flushes on size or timeout during the test, so only shutdown drains it.
    private static BatchProcessorConfig holdingBatch() {
        return new BatchProcessorConfig(
                new BatchPolicy(1_000_000, 0, 0, Duration.ofMinutes(10), List.of()),
                new DeliveryQueuePolicy(1024, 1, OverflowPolicy.FAIL),
                Duration.ofSeconds(5),
                BatchProcessorConfig.DEFAULT_MAX_PENDING_SUBMISSIONS,
                BatchProcessorConfig.DEFAULT_MAX_PENDING_BYTES,
                BatchProcessorConfig.DEFAULT_MAX_PARTITIONS);
    }

    private static ExporterFactory<ComponentConfig.None> capturingInto(List<Payload<?>> sink) {
        return ExporterFactory.of("capture", (ctx, data, provenance) -> {
            sink.add(data);
            return DeliveryResult.acceptedStage(provenance);
        });
    }

    private static Payload<TracesData> tracesFor(String serviceName, int spanCount) {
        var spans = new ArrayList<Span>();
        for (var i = 0; i < spanCount; i++) {
            spans.add(Fixtures.span("span-" + i, Span.Kind.SERVER));
        }
        var resource = Resource.of(Attributes.builder().put("service.name", serviceName).build());
        return Payload.of(TRACES, TracesData.of(resource, InstrumentationScope.EMPTY, spans));
    }
}
