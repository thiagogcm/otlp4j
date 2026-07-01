package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.ExponentialHistogramPoint;
import dev.nthings.otlp4j.model.Metric;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.testing.Fixtures;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsPartialSuccess;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsPartialSuccess;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesPartialSuccess;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.ExportTracePartialSuccess;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.metrics.v1.Histogram;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import java.util.List;
import java.util.OptionalDouble;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// White-box unit tests for mapper behaviour that the round-trip properties cannot express.
@DisplayName("Mapper edge cases")
class MapperEdgeCaseTest {

    @DisplayName("ExponentialHistogram bucket offset survives round-trip with empty counts")
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
                0L,
                List.of());
        var sent = Fixtures.metricsData(Metric.builder()
                .name("exp.histogram")
                .data(new Metric.ExponentialHistogram(
                        List.of(point), Metric.AggregationTemporality.DELTA))
                .build());

        var roundTripped = MetricsMapper.toDomain(MetricsMapper.toProto(sent));

        assertThat(roundTripped).isEqualTo(sent);
    }

    @DisplayName("A malformed inbound histogram point (buckets != bounds + 1) is rejected on decode")
    @Test
    void aMalformedHistogramWirePointIsRejectedOnDecode() {
        // 2 bucket counts with 0 explicit bounds violates the buckets == bounds + 1 invariant.
        var request = ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(ResourceMetrics.newBuilder()
                        .addScopeMetrics(ScopeMetrics.newBuilder()
                                .addMetrics(io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                                        .setName("bad.histogram")
                                        .setHistogram(Histogram.newBuilder()
                                                .addDataPoints(HistogramDataPoint.newBuilder()
                                                        .addBucketCounts(1L)
                                                        .addBucketCounts(2L))))))
                .build();

        assertThatThrownBy(() -> MetricsMapper.toDomain(request))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("Metric with no data maps back to the empty NoData form")
    @Test
    void aMetricWithNoDataMapsBackToNoData() {
        var sent = Fixtures.metricsData(Metric.builder().name("placeholder.metric").build());

        var roundTripped = MetricsMapper.toDomain(MetricsMapper.toProto(sent)).metrics().getFirst();

        assertThat(roundTripped.name()).isEqualTo("placeholder.metric");
        assertThat(roundTripped.data()).isEqualTo(Metric.noData());
        assertThat(roundTripped.hasData()).isFalse();
    }

    @DisplayName("TraceMapper.result reads rejected count and message from partial_success")
    @Test
    void traceResultReadsThePartialSuccessFields() {
        var response = ExportTraceServiceResponse.newBuilder()
                .setPartialSuccess(ExportTracePartialSuccess.newBuilder()
                        .setRejectedSpans(2)
                        .setErrorMessage("2 spans malformed"))
                .build();

        var result = TraceMapper.result(response);

        assertThat(result).isInstanceOf(ConsumeResult.Partial.class);
        var p = (ConsumeResult.Partial) result;
        assertThat(p.rejectedItems()).isEqualTo(2);
        assertThat(p.message()).isEqualTo("2 spans malformed");
    }

    @DisplayName("TraceMapper.result with no partial_success is Accepted")
    @Test
    void traceResultWithNoPartialSuccessIsAccepted() {
        assertThat(TraceMapper.result(ExportTraceServiceResponse.getDefaultInstance()))
                .isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("LogsMapper.result maps partial_success to Accepted, Partial, or Rejected")
    @Test
    void logsResultDecisionTable() {
        assertThat(LogsMapper.result(ExportLogsServiceResponse.getDefaultInstance()))
                .as("no partial_success block -> accepted")
                .isInstanceOf(ConsumeResult.Accepted.class);

        assertThat(LogsMapper.result(ExportLogsServiceResponse.newBuilder()
                        .setPartialSuccess(ExportLogsPartialSuccess.getDefaultInstance())
                        .build()))
                .as("empty partial_success block -> accepted")
                .isInstanceOf(ConsumeResult.Accepted.class);

        var withCount = LogsMapper.result(ExportLogsServiceResponse.newBuilder()
                .setPartialSuccess(ExportLogsPartialSuccess.newBuilder()
                        .setRejectedLogRecords(3)
                        .setErrorMessage("3 records rejected"))
                .build());
        assertThat(withCount).isInstanceOf(ConsumeResult.Partial.class);
        var p = (ConsumeResult.Partial) withCount;
        assertThat(p.rejectedItems()).isEqualTo(3);
        assertThat(p.message()).isEqualTo("3 records rejected");

        var messageOnly = LogsMapper.result(ExportLogsServiceResponse.newBuilder()
                .setPartialSuccess(ExportLogsPartialSuccess.newBuilder()
                        .setErrorMessage("log warning"))
                .build());
        assertThat(messageOnly).isInstanceOf(ConsumeResult.Rejected.class);
        var r = (ConsumeResult.Rejected) messageOnly;
        assertThat(r.message()).isEqualTo("log warning");
    }

    @DisplayName("TraceMapper.result with empty partial_success block is Accepted")
    @Test
    void traceResultWithAnEmptyPartialSuccessBlockIsAccepted() {
        var response = ExportTraceServiceResponse.newBuilder()
                .setPartialSuccess(ExportTracePartialSuccess.getDefaultInstance())
                .build();
        assertThat(TraceMapper.result(response)).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("TraceMapper.result with message only and zero count is Rejected")
    @Test
    void traceResultWithAMessageOnlyIsRejected() {
        var response = ExportTraceServiceResponse.newBuilder()
                .setPartialSuccess(ExportTracePartialSuccess.newBuilder()
                        .setErrorMessage("warning: clock skew detected"))
                .build();

        var result = TraceMapper.result(response);
        assertThat(result).isInstanceOf(ConsumeResult.Rejected.class);
        assertThat(((ConsumeResult.Rejected) result).message()).isEqualTo("warning: clock skew detected");
    }

    @DisplayName("MetricsMapper.result maps partial_success to Accepted, Partial, or Rejected")
    @Test
    void metricsResultDecisionTable() {
        assertThat(MetricsMapper.result(ExportMetricsServiceResponse.getDefaultInstance()))
                .isInstanceOf(ConsumeResult.Accepted.class);

        assertThat(MetricsMapper.result(ExportMetricsServiceResponse.newBuilder()
                        .setPartialSuccess(ExportMetricsPartialSuccess.getDefaultInstance())
                        .build()))
                .isInstanceOf(ConsumeResult.Accepted.class);

        var withCount = MetricsMapper.result(ExportMetricsServiceResponse.newBuilder()
                .setPartialSuccess(ExportMetricsPartialSuccess.newBuilder()
                        .setRejectedDataPoints(7)
                        .setErrorMessage("7 points rejected"))
                .build());
        assertThat(withCount).isInstanceOf(ConsumeResult.Partial.class);
        var p = (ConsumeResult.Partial) withCount;
        assertThat(p.rejectedItems()).isEqualTo(7);
        assertThat(p.message()).isEqualTo("7 points rejected");

        var messageOnly = MetricsMapper.result(ExportMetricsServiceResponse.newBuilder()
                .setPartialSuccess(ExportMetricsPartialSuccess.newBuilder()
                        .setErrorMessage("metadata warning"))
                .build());
        assertThat(messageOnly).isInstanceOf(ConsumeResult.Rejected.class);
    }

    @DisplayName("ProfilesMapper.result maps partial_success to Accepted, Partial, or Rejected")
    @Test
    void profilesResultDecisionTable() {
        assertThat(ProfilesMapper.result(ExportProfilesServiceResponse.getDefaultInstance()))
                .isInstanceOf(ConsumeResult.Accepted.class);

        assertThat(ProfilesMapper.result(ExportProfilesServiceResponse.newBuilder()
                        .setPartialSuccess(ExportProfilesPartialSuccess.getDefaultInstance())
                        .build()))
                .isInstanceOf(ConsumeResult.Accepted.class);

        var withCount = ProfilesMapper.result(ExportProfilesServiceResponse.newBuilder()
                .setPartialSuccess(ExportProfilesPartialSuccess.newBuilder()
                        .setRejectedProfiles(4)
                        .setErrorMessage("4 profiles rejected"))
                .build());
        assertThat(withCount).isInstanceOf(ConsumeResult.Partial.class);
        var p = (ConsumeResult.Partial) withCount;
        assertThat(p.rejectedItems()).isEqualTo(4);

        var messageOnly = ProfilesMapper.result(ExportProfilesServiceResponse.newBuilder()
                .setPartialSuccess(ExportProfilesPartialSuccess.newBuilder()
                        .setErrorMessage("profiles warning"))
                .build());
        assertThat(messageOnly).isInstanceOf(ConsumeResult.Rejected.class);
    }
}
