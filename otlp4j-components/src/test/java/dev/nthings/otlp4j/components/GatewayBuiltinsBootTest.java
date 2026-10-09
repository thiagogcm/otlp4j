package dev.nthings.otlp4j.components;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.components.batch.BatchProcessorConfig;
import dev.nthings.otlp4j.components.batch.BatchProcessorFactory;
import dev.nthings.otlp4j.components.memory.MemoryLimiterProcessorFactory;
import dev.nthings.otlp4j.components.memory.MemoryLimiterConfig;
import dev.nthings.otlp4j.components.routing.FilterConfig;
import dev.nthings.otlp4j.components.routing.FilterProcessorFactory;
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

/// Boots a real [dev.nthings.otlp4j.service.GatewayRuntime] wiring the built-in processors - memory
/// limiter, filter, and batch - fed by an in-process receiver and terminated by capturing exporters,
/// and proves data flows through every one of them: the memory limiter admits under the configured
/// threshold, two filter branches split one mixed-resource payload so each exporter receives exactly
/// its own service, and the batch stage holds its data until a clean shutdown flushes it through the
/// still-open filter and exporter downstream of it.
@DisplayName("Built-in components booted in a gateway")
@Timeout(30)
class GatewayBuiltinsBootTest {

    private static final ComponentId RECV = ComponentId.parse("in");
    private static final ComponentId MEMLIMIT = ComponentId.parse("memlimit");
    private static final ComponentId KEEP_CHECKOUT = ComponentId.parse("keep-checkout");
    private static final ComponentId KEEP_BILLING = ComponentId.parse("keep-billing");
    private static final ComponentId BATCH = ComponentId.parse("batch");
    private static final ComponentId EXP_CHECKOUT = ComponentId.parse("out-checkout");
    private static final ComponentId EXP_BILLING = ComponentId.parse("out-billing");

    @DisplayName("filter branches split a mixed-resource payload; batch flushes on shutdown")
    @Test
    void builtInsCarryDataAndBatchDrainsOnShutdown() {
        var checkoutReceived = new CopyOnWriteArrayList<Payload<?>>();
        var billingReceived = new CopyOnWriteArrayList<Payload<?>>();
        var recv = new GraphReceiverFactory();

        var gw = Gateway.builder();
        var in = gw.receiver(RECV, recv);
        var memlimit = gw.processor(MEMLIMIT, new MemoryLimiterProcessorFactory(),
                new MemoryLimiterConfig(Duration.ofSeconds(1), Long.MAX_VALUE, 0, Duration.ofSeconds(1)));
        var keepCheckout = gw.processor(KEEP_CHECKOUT, new FilterProcessorFactory(), keepService("checkout"));
        var keepBilling = gw.processor(KEEP_BILLING, new FilterProcessorFactory(), keepService("billing"));
        var batch = gw.processor(BATCH, new BatchProcessorFactory(), holdingBatch());
        var checkoutOut = gw.exporter(EXP_CHECKOUT, capturingInto(checkoutReceived));
        var billingOut = gw.exporter(EXP_BILLING, capturingInto(billingReceived));

        // Two branches off the same receiver, each with its own filter. The checkout branch runs
        // through the memory limiter and buffers before it filters, so its filter is a processor
        // downstream of the batch stage.
        gw.pipeline(PipelineId.of(TRACES, "checkout"),
                p -> p.from(in).via(memlimit, batch, keepCheckout).to(checkoutOut));
        gw.pipeline(PipelineId.of(TRACES, "billing"),
                p -> p.from(in).via(keepBilling).to(billingOut));
        var runtime = gw.build();
        runtime.start();

        // One payload mixing both services, billing first: each branch must take only its own.
        var mixed = Payload.of(TRACES, new TracesData(List.of(
                resourceSpans("billing", 2), resourceSpans("checkout", 3))));
        var result = recv.deliver(TRACES, new TestConsumeContext(), mixed);

        // Admitted: the memory limiter is under the configured threshold.
        assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);

        // The billing branch forwarded only the billing resource; the checkout branch's batch stage
        // is still holding the payload.
        assertThat(billingReceived).singleElement().satisfies(payload -> {
            assertThat(payload.itemCount()).isEqualTo(2L);
            assertThat(services(payload)).containsExactly("billing");
        });
        assertThat(checkoutReceived).as("batch holds the payload until shutdown").isEmpty();

        runtime.close();

        // Shutdown drained the batch through the still-open filter, which dropped billing, into the
        // exporter.
        assertThat(checkoutReceived).as("batch flushes pending data on clean shutdown").singleElement()
                .satisfies(payload -> {
                    assertThat(payload.itemCount()).isEqualTo(3L);
                    assertThat(services(payload)).containsExactly("checkout");
                });
        assertThat(billingReceived).hasSize(1);
    }

    /// A branch filter that keeps only resource groups whose `service.name` equals `service`.
    private static FilterConfig keepService(String service) {
        return FilterConfig.keep(
                MatchSpec.of(MatchRule.builder().resourceAttribute("service.name", service).build()));
    }

    private static List<String> services(Payload<?> payload) {
        return payload.routingMetadata().resources().stream()
                .map(attributes -> attributes.getString("service.name"))
                .toList();
    }

    /// A batch config that never flushes on size or timeout during the test, so only shutdown drains it.
    private static BatchProcessorConfig holdingBatch() {
        return new BatchProcessorConfig(
                new BatchPolicy(1_000_000, 0, 0, Duration.ZERO, List.of()),
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

    private static TracesData.ResourceSpans resourceSpans(String serviceName, int spanCount) {
        var spans = new ArrayList<Span>();
        for (var i = 0; i < spanCount; i++) {
            spans.add(Fixtures.span("span-" + i, Span.Kind.SERVER));
        }
        var resource = Resource.of(Attributes.builder().put("service.name", serviceName).build());
        return new TracesData.ResourceSpans(
                resource, "", List.of(new TracesData.ScopeSpans(InstrumentationScope.EMPTY, "", spans)));
    }
}
