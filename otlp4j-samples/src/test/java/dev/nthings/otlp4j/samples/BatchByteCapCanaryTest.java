package dev.nthings.otlp4j.samples;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.components.batch.BatchProcessorConfig;
import dev.nthings.otlp4j.components.batch.BatchProcessorFactory;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.policy.BatchPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.service.Pipeline;
import dev.nthings.otlp4j.transport.OtlpPayloadSizer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Integration canary for the one configuration that joins the batch processor to the transport: a
/// hard output-byte cap enforced with the exact OTLP sizer.
@Timeout(30)
@DisplayName("Batch byte cap with the exact OTLP sizer")
class BatchByteCapCanaryTest {

    private static final long CAP_BYTES = 4096;

    @DisplayName("an in-process batch is split into requests within the cap without losing a span")
    @Test
    void splitsAnInProcessBatchIntoRequestsWithinTheCap() {
        var requestSizes = new CopyOnWriteArrayList<Long>();
        var spans = new AtomicLong();
        var sink = ExporterFactory.of("capture", (context, payload, provenance) -> {
            requestSizes.add(OtlpPayloadSizer.exact().sizeOf(payload));
            spans.addAndGet(payload.itemCount());
            return DeliveryResult.acceptedStage(provenance);
        });
        var config = new BatchProcessorConfig(
                new BatchPolicy(8192, 0, CAP_BYTES, Duration.ZERO, List.of()),
                new DeliveryQueuePolicy(1024, 1, OverflowPolicy.FAIL),
                Duration.ofSeconds(5),
                BatchProcessorConfig.DEFAULT_MAX_PENDING_SUBMISSIONS,
                BatchProcessorConfig.DEFAULT_MAX_PENDING_BYTES,
                BatchProcessorConfig.DEFAULT_MAX_PARTITIONS,
                BatchProcessorConfig.DEFAULT_MAX_PENDING_ITEMS,
                OtlpPayloadSizer.exact());

        try (var pipeline = Pipeline.builder(TRACES)
                .via(new BatchProcessorFactory(), config)
                .to(sink)
                .build()) {
            pipeline.start();
            var admission = pipeline.submit(traces(1000)).toCompletableFuture().join();
            var report = pipeline.flush(Duration.ofSeconds(10)).toCompletableFuture().join();

            assertThat(admission).isInstanceOf(DeliveryResult.Accepted.class);
            assertThat(report.isSuccess()).isTrue();
        }

        assertThat(spans).hasValue(1000);
        assertThat(requestSizes).hasSizeGreaterThan(1)
                .allSatisfy(size -> assertThat(size).isPositive().isLessThanOrEqualTo(CAP_BYTES));
    }

    private static TracesData traces(int count) {
        var spans = new ArrayList<Span>(count);
        for (var i = 0; i < count; i++) {
            spans.add(Span.builder()
                    .traceId("0102030405060708090a0b0c0d0e0f10")
                    .spanId("0102030405060708")
                    .name("span-" + i)
                    .kind(Span.Kind.SERVER)
                    .startEpochNanos(1_000L)
                    .endEpochNanos(2_000L)
                    .build());
        }
        return TracesData.of(
                Resource.of(Attributes.builder().put("service.name", "checkout").build()),
                InstrumentationScope.of("canary", "1.0.0"),
                spans);
    }
}
