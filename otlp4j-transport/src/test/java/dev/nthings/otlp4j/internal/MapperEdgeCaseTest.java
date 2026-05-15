package dev.nthings.otlp4j.internal;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.ExponentialHistogramPoint;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.testing.Fixtures;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsPartialSuccess;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsPartialSuccess;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesPartialSuccess;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.ExportTracePartialSuccess;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import java.util.List;
import java.util.OptionalDouble;
import org.junit.jupiter.api.Test;

/// White-box unit tests for mapper behaviour that the round-trip properties cannot express:
/// boundary cases and the intentionally lossy parts of the contract.
class MapperEdgeCaseTest {

    @Test
    void exponentialHistogramBucketOffsetSurvivesRoundTripWhenCountsAreEmpty() {
        var point = new ExponentialHistogramPoint(
                Attributes.empty(),
                0L,
                0L,
                0L,
                OptionalDouble.empty(),
                0,
                0L,
                new ExponentialHistogramPoint.Buckets(7, List.of()),
                new ExponentialHistogramPoint.Buckets(-3, List.of()),
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                0.0,
                0L);
        var sent = Fixtures.metricsData(Metric.builder()
                .name("exp.histogram")
                .data(new Metric.ExponentialHistogram(
                        List.of(point), Metric.AggregationTemporality.DELTA))
                .build());

        var roundTripped = MetricsMapper.toDomain(MetricsMapper.toProto(sent));

        assertThat(roundTripped)
                .as("a non-zero bucket offset must survive even when bucketCounts is empty")
                .isEqualTo(sent);
    }

    @Test
    void aMetricWithNoDataMapsBackToNull() {
        var sent = Fixtures.metricsData(Metric.builder().name("placeholder.metric").build());

        var roundTripped = MetricsMapper.toDomain(MetricsMapper.toProto(sent)).metrics().get(0);

        assertThat(roundTripped.name()).isEqualTo("placeholder.metric");
        assertThat(roundTripped.data())
                .as("an unset metric data oneof must map back to null")
                .isNull();
    }

    @Test
    void profilesMapperDropsSampleCountButPreservesMetadata() {
        var sent = TransportFixtures.profiles(
                new ProfilesData.Profile("0102030405060708", 100L, 200L, 5L, 42, 3, "pprof"));

        var roundTripped = ProfilesMapper.toDomain(ProfilesMapper.toProto(sent)).profiles().get(0);

        assertThat(roundTripped.sampleCount())
                .as("the sample/location/string tables are intentionally not reconstructed")
                .isZero();
        assertThat(roundTripped.profileId()).isEqualTo("0102030405060708");
        assertThat(roundTripped.timeUnixNano()).isEqualTo(100L);
        assertThat(roundTripped.durationNanos()).isEqualTo(200L);
        assertThat(roundTripped.period()).isEqualTo(5L);
        assertThat(roundTripped.droppedAttributesCount()).isEqualTo(3);
        assertThat(roundTripped.originalPayloadFormat()).isEqualTo("pprof");
    }

    @Test
    void traceResultReadsThePartialSuccessFields() {
        var response = ExportTraceServiceResponse.newBuilder()
                .setPartialSuccess(ExportTracePartialSuccess.newBuilder()
                        .setRejectedSpans(2)
                        .setErrorMessage("2 spans malformed"))
                .build();

        var result = TraceMapper.result(response);

        assertThat(result.rejectedCount()).isEqualTo(2);
        assertThat(result.message()).isEqualTo("2 spans malformed");
    }

    @Test
    void traceResultWithNoPartialSuccessIsFullSuccess() {
        var result = TraceMapper.result(ExportTraceServiceResponse.getDefaultInstance());

        assertThat(result.isFullSuccess()).isTrue();
    }

    @Test
    void logsResultDecisionTable() {
        assertThat(LogsMapper.result(ExportLogsServiceResponse.getDefaultInstance())
                        .isFullSuccess())
                .as("no partial_success block -> full success")
                .isTrue();

        assertThat(LogsMapper.result(ExportLogsServiceResponse.newBuilder()
                                .setPartialSuccess(ExportLogsPartialSuccess.getDefaultInstance())
                                .build())
                        .isFullSuccess())
                .as("empty partial_success block -> full success")
                .isTrue();

        var withCount = LogsMapper.result(ExportLogsServiceResponse.newBuilder()
                .setPartialSuccess(ExportLogsPartialSuccess.newBuilder()
                        .setRejectedLogRecords(3)
                        .setErrorMessage("3 records rejected"))
                .build());
        assertThat(withCount.rejectedCount()).isEqualTo(3);
        assertThat(withCount.message()).isEqualTo("3 records rejected");

        var messageOnly = LogsMapper.result(ExportLogsServiceResponse.newBuilder()
                .setPartialSuccess(ExportLogsPartialSuccess.newBuilder()
                        .setErrorMessage("log warning"))
                .build());
        assertThat(messageOnly.isFullSuccess()).isFalse();
        assertThat(messageOnly.rejectedCount()).isZero();
        assertThat(messageOnly.message()).isEqualTo("log warning");
    }

    @Test
    void traceResultWithAnEmptyPartialSuccessBlockIsFullSuccess() {
        // partial_success is set, but with 0 rejected spans and no message: the collector signalled
        // nothing was actually rejected, so this collapses to a full success.
        var response = ExportTraceServiceResponse.newBuilder()
                .setPartialSuccess(ExportTracePartialSuccess.getDefaultInstance())
                .build();

        assertThat(TraceMapper.result(response).isFullSuccess())
                .as("a present-but-empty partial_success block must collapse to full success")
                .isTrue();
    }

    @Test
    void traceResultWithAMessageOnlyIsAPartialSuccess() {
        // A message with 0 rejected spans is still a partial success — the message is non-empty
        // so the short-circuit does not fire.
        var response = ExportTraceServiceResponse.newBuilder()
                .setPartialSuccess(ExportTracePartialSuccess.newBuilder()
                        .setErrorMessage("warning: clock skew detected"))
                .build();

        var result = TraceMapper.result(response);

        assertThat(result.isFullSuccess()).isFalse();
        assertThat(result.rejectedCount()).isZero();
        assertThat(result.message()).isEqualTo("warning: clock skew detected");
    }

    @Test
    void metricsResultDecisionTable() {
        assertThat(MetricsMapper.result(ExportMetricsServiceResponse.getDefaultInstance())
                        .isFullSuccess())
                .as("no partial_success block -> full success")
                .isTrue();

        assertThat(MetricsMapper.result(ExportMetricsServiceResponse.newBuilder()
                                .setPartialSuccess(ExportMetricsPartialSuccess.getDefaultInstance())
                                .build())
                        .isFullSuccess())
                .as("empty partial_success block -> full success")
                .isTrue();

        var withCount = MetricsMapper.result(ExportMetricsServiceResponse.newBuilder()
                .setPartialSuccess(ExportMetricsPartialSuccess.newBuilder()
                        .setRejectedDataPoints(7)
                        .setErrorMessage("7 points rejected"))
                .build());
        assertThat(withCount.rejectedCount()).isEqualTo(7);
        assertThat(withCount.message()).isEqualTo("7 points rejected");

        var messageOnly = MetricsMapper.result(ExportMetricsServiceResponse.newBuilder()
                .setPartialSuccess(ExportMetricsPartialSuccess.newBuilder()
                        .setErrorMessage("metadata warning"))
                .build());
        assertThat(messageOnly.isFullSuccess()).isFalse();
        assertThat(messageOnly.rejectedCount()).isZero();
        assertThat(messageOnly.message()).isEqualTo("metadata warning");
    }

    @Test
    void profilesResultDecisionTable() {
        assertThat(ProfilesMapper.result(ExportProfilesServiceResponse.getDefaultInstance())
                        .isFullSuccess())
                .as("no partial_success block -> full success")
                .isTrue();

        assertThat(ProfilesMapper.result(ExportProfilesServiceResponse.newBuilder()
                                .setPartialSuccess(
                                        ExportProfilesPartialSuccess.getDefaultInstance())
                                .build())
                        .isFullSuccess())
                .as("empty partial_success block -> full success")
                .isTrue();

        var withCount = ProfilesMapper.result(ExportProfilesServiceResponse.newBuilder()
                .setPartialSuccess(ExportProfilesPartialSuccess.newBuilder()
                        .setRejectedProfiles(4)
                        .setErrorMessage("4 profiles rejected"))
                .build());
        assertThat(withCount.rejectedCount()).isEqualTo(4);
        assertThat(withCount.message()).isEqualTo("4 profiles rejected");

        var messageOnly = ProfilesMapper.result(ExportProfilesServiceResponse.newBuilder()
                .setPartialSuccess(ExportProfilesPartialSuccess.newBuilder()
                        .setErrorMessage("profiles warning"))
                .build());
        assertThat(messageOnly.isFullSuccess()).isFalse();
        assertThat(messageOnly.rejectedCount()).isZero();
        assertThat(messageOnly.message()).isEqualTo("profiles warning");
    }

    @Test
    void profilesMapperPreservesEveryScalarMetadataFieldExceptSampleCount() {
        // Companion to profilesMapperDropsSampleCountButPreservesMetadata: this asserts the full
        // scalar metadata surface survives, using distinct non-zero values for every field so a
        // field swap in the mapper would be caught.
        var sent = TransportFixtures.profiles(new ProfilesData.Profile(
                "aabbccddeeff00112233445566778899", 111L, 222L, 333L, 444, 555, "pprof-gz"));

        var roundTripped = ProfilesMapper.toDomain(ProfilesMapper.toProto(sent)).profiles().get(0);

        assertThat(roundTripped.profileId()).isEqualTo("aabbccddeeff00112233445566778899");
        assertThat(roundTripped.timeUnixNano()).isEqualTo(111L);
        assertThat(roundTripped.durationNanos()).isEqualTo(222L);
        assertThat(roundTripped.period()).isEqualTo(333L);
        assertThat(roundTripped.droppedAttributesCount()).isEqualTo(555);
        assertThat(roundTripped.originalPayloadFormat()).isEqualTo("pprof-gz");
        assertThat(roundTripped.sampleCount())
                .as("sampleCount is intentionally dropped: the sample table is not reconstructed")
                .isZero();
    }
}
