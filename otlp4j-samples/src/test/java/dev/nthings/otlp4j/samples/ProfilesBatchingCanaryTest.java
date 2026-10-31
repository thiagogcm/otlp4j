package dev.nthings.otlp4j.samples;

import static dev.nthings.otlp4j.core.signal.SignalDescriptor.PROFILES;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.components.batch.BatchProcessorConfig;
import dev.nthings.otlp4j.components.batch.BatchProcessorFactory;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.policy.BatchPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Profile;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.service.Pipeline;
import dev.nthings.otlp4j.transport.OtlpPayloadSizer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Integration canary for profile pipelines, whose items vary too much in size for the item-based
/// defaults to mean anything: a profile counts as one item whatever it holds, so these guarantees all
/// come from the byte limits the exact OTLP sizer enables.
@Timeout(30)
@DisplayName("Profile batching with the exact OTLP sizer")
class ProfilesBatchingCanaryTest {

    private static final long MIB = 1024 * 1024;
    private static final AtomicLong NEXT_ID = new AtomicLong();

    private static BatchProcessorConfig config(long maxRequestBytes, long maxPendingBytes) {
        return new BatchProcessorConfig(
                new BatchPolicy(8192, 0, maxRequestBytes, Duration.ZERO, List.of()),
                new DeliveryQueuePolicy(1024, 1, OverflowPolicy.FAIL),
                Duration.ofSeconds(5),
                BatchProcessorConfig.DEFAULT_MAX_PENDING_SUBMISSIONS,
                maxPendingBytes,
                BatchProcessorConfig.DEFAULT_MAX_PARTITIONS,
                BatchProcessorConfig.DEFAULT_MAX_PENDING_ITEMS,
                OtlpPayloadSizer.exact());
    }

    @DisplayName("requests stay within the byte cap, each with only the dictionary it needs, losing no profile")
    @Test
    void splitsProfilesIntoRequestsWithinTheCap() {
        var requestSizes = new CopyOnWriteArrayList<Long>();
        var exported = new CopyOnWriteArrayList<Profile>();
        var sink = ExporterFactory.of("capture", (context, payload, provenance) -> {
            requestSizes.add(OtlpPayloadSizer.exact().sizeOf(payload));
            exported.addAll(((ProfilesData) payload.data()).profiles());
            return DeliveryResult.acceptedStage(provenance);
        });
        var first = batch(40, 16 * 1024);
        var second = batch(40, 16 * 1024);

        try (var pipeline = Pipeline.builder(PROFILES)
                .via(new BatchProcessorFactory(), config(64 * 1024, 16 * MIB))
                .to(sink)
                .build()) {
            pipeline.start();
            assertThat(pipeline.submit(first).toCompletableFuture().join()).isInstanceOf(DeliveryResult.Accepted.class);
            assertThat(pipeline.submit(second).toCompletableFuture().join()).isInstanceOf(DeliveryResult.Accepted.class);

            assertThat(pipeline.flush(Duration.ofSeconds(10)).toCompletableFuture().join().isSuccess()).isTrue();
        }

        assertThat(requestSizes).hasSizeGreaterThan(1)
                .allSatisfy(size -> assertThat(size).isPositive().isLessThanOrEqualTo(64 * 1024));
        var sent = new ArrayList<>(first.profiles());
        sent.addAll(second.profiles());
        assertThat(exported).containsExactlyElementsOf(sent);
    }

    @DisplayName("a profile built in process counts its real size against the pending byte limit")
    @Test
    void chargesInProcessProfilesInBytes() {
        var large = batch(1, (int) (2 * MIB));

        try (var pipeline = Pipeline.builder(PROFILES)
                .via(new BatchProcessorFactory(), config(0, 3 * MIB))
                .to(ExporterFactory.of("capture", (context, payload, provenance) ->
                        DeliveryResult.acceptedStage(provenance)))
                .build()) {
            pipeline.start();

            assertThat(pipeline.submit(large).toCompletableFuture().join()).isInstanceOf(DeliveryResult.Accepted.class);
            assertThat(pipeline.submit(large).toCompletableFuture().join()).isInstanceOf(DeliveryResult.Rejected.class);
        }
    }

    @DisplayName("a single profile above the byte cap is refused, because a profile does not split")
    @Test
    void refusesAProfileAboveTheCap() {
        try (var pipeline = Pipeline.builder(PROFILES)
                .via(new BatchProcessorFactory(), config(64 * 1024, 16 * MIB))
                .to(ExporterFactory.of("capture", (context, payload, provenance) ->
                        DeliveryResult.acceptedStage(provenance)))
                .build()) {
            pipeline.start();

            assertThat(pipeline.submit(batch(1, 128 * 1024)).toCompletableFuture().join())
                    .isInstanceOf(DeliveryResult.Rejected.class);
        }
    }

    /// `count` profiles, each carrying `payloadBytes` of distinct original payload and its own function,
    /// so that no two share a dictionary entry beyond the zero values.
    private static ProfilesData batch(int count, int payloadBytes) {
        var profiles = new ArrayList<Profile>(count);
        for (var i = 0; i < count; i++) {
            var id = HexFormat.of().toHexDigits(NEXT_ID.incrementAndGet()).repeat(2);
            var frame = Profile.Location.builder()
                    .addLine(new Profile.Line(new Profile.Function("function-" + id, "", "", 0L), 1L, 0L))
                    .build();
            profiles.add(Profile.builder()
                    .profileId(id)
                    .sampleType(new Profile.ValueType("cpu", "nanoseconds"))
                    .addSample(Profile.Sample.builder()
                            .stack(new Profile.Stack(List.of(frame)))
                            .addValue(1L)
                            .build())
                    .originalPayloadFormat("pprof")
                    .originalPayload(new byte[payloadBytes])
                    .build());
        }
        return ProfilesData.of(Resource.EMPTY, InstrumentationScope.of("profiler"), profiles);
    }
}
