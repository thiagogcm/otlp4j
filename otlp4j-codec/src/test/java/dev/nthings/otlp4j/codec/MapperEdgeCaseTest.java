package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.ExponentialHistogramPoint;
import dev.nthings.otlp4j.model.Metric;
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

    private static final Provenance PROV =
            new Provenance(new ComponentId("otlp", "e"), ComponentKind.EXPORTER, SignalDescriptor.TRACES);

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

    @DisplayName("TraceMapper.deliveryResult reads rejected count and message from partial_success")
    @Test
    void traceDeliveryResultReadsThePartialSuccessFields() {
        var response = ExportTraceServiceResponse.newBuilder()
                .setPartialSuccess(ExportTracePartialSuccess.newBuilder()
                        .setRejectedSpans(2)
                        .setErrorMessage("2 spans malformed"))
                .build();

        var result = TraceMapper.deliveryResult(response, PROV);

        assertThat(result).isInstanceOf(DeliveryResult.Partial.class);
        var p = (DeliveryResult.Partial) result;
        assertThat(p.rejectedItems()).isEqualTo(2);
        assertThat(p.message()).isEqualTo("2 spans malformed");
    }

    @DisplayName("TraceMapper.deliveryResult with no partial_success is Accepted")
    @Test
    void traceDeliveryResultWithNoPartialSuccessIsAccepted() {
        assertThat(TraceMapper.deliveryResult(ExportTraceServiceResponse.getDefaultInstance(), PROV))
                .isInstanceOf(DeliveryResult.Accepted.class);
    }

    @DisplayName("LogsMapper.deliveryResult preserves every present partial_success block")
    @Test
    void logsDeliveryResultDecisionTable() {
        assertThat(LogsMapper.deliveryResult(ExportLogsServiceResponse.getDefaultInstance(), PROV))
                .as("no partial_success block -> accepted")
                .isInstanceOf(DeliveryResult.Accepted.class);

        assertThat(LogsMapper.deliveryResult(ExportLogsServiceResponse.newBuilder()
                        .setPartialSuccess(ExportLogsPartialSuccess.getDefaultInstance())
                        .build(), PROV))
                .as("empty partial_success block -> terminal warning")
                .isEqualTo(new DeliveryResult.Partial(0, "", PROV));

        var withCount = LogsMapper.deliveryResult(ExportLogsServiceResponse.newBuilder()
                .setPartialSuccess(ExportLogsPartialSuccess.newBuilder()
                        .setRejectedLogRecords(3)
                        .setErrorMessage("3 records rejected"))
                .build(), PROV);
        assertThat(withCount).isInstanceOf(DeliveryResult.Partial.class);
        var p = (DeliveryResult.Partial) withCount;
        assertThat(p.rejectedItems()).isEqualTo(3);
        assertThat(p.message()).isEqualTo("3 records rejected");

        var messageOnly = LogsMapper.deliveryResult(ExportLogsServiceResponse.newBuilder()
                .setPartialSuccess(ExportLogsPartialSuccess.newBuilder()
                        .setErrorMessage("log warning"))
                .build(), PROV);
        assertThat(messageOnly).isEqualTo(new DeliveryResult.Partial(0, "log warning", PROV));
    }

    @DisplayName("TraceMapper.deliveryResult with empty partial_success block is Partial")
    @Test
    void traceDeliveryResultWithAnEmptyPartialSuccessBlockIsAccepted() {
        var response = ExportTraceServiceResponse.newBuilder()
                .setPartialSuccess(ExportTracePartialSuccess.getDefaultInstance())
                .build();
        assertThat(TraceMapper.deliveryResult(response, PROV))
                .isEqualTo(new DeliveryResult.Partial(0, "", PROV));
    }

    @DisplayName("TraceMapper.deliveryResult with message only and zero count is Partial")
    @Test
    void traceDeliveryResultWithAMessageOnlyIsRejected() {
        var response = ExportTraceServiceResponse.newBuilder()
                .setPartialSuccess(ExportTracePartialSuccess.newBuilder()
                        .setErrorMessage("warning: clock skew detected"))
                .build();

        var result = TraceMapper.deliveryResult(response, PROV);
        assertThat(result)
                .isEqualTo(new DeliveryResult.Partial(0, "warning: clock skew detected", PROV));
    }

    @DisplayName("MetricsMapper.deliveryResult preserves every present partial_success block")
    @Test
    void metricsDeliveryResultDecisionTable() {
        assertThat(MetricsMapper.deliveryResult(ExportMetricsServiceResponse.getDefaultInstance(), PROV))
                .isInstanceOf(DeliveryResult.Accepted.class);

        assertThat(MetricsMapper.deliveryResult(ExportMetricsServiceResponse.newBuilder()
                        .setPartialSuccess(ExportMetricsPartialSuccess.getDefaultInstance())
                        .build(), PROV))
                .isEqualTo(new DeliveryResult.Partial(0, "", PROV));

        var withCount = MetricsMapper.deliveryResult(ExportMetricsServiceResponse.newBuilder()
                .setPartialSuccess(ExportMetricsPartialSuccess.newBuilder()
                        .setRejectedDataPoints(7)
                        .setErrorMessage("7 points rejected"))
                .build(), PROV);
        assertThat(withCount).isInstanceOf(DeliveryResult.Partial.class);
        var p = (DeliveryResult.Partial) withCount;
        assertThat(p.rejectedItems()).isEqualTo(7);
        assertThat(p.message()).isEqualTo("7 points rejected");

        var messageOnly = MetricsMapper.deliveryResult(ExportMetricsServiceResponse.newBuilder()
                .setPartialSuccess(ExportMetricsPartialSuccess.newBuilder()
                        .setErrorMessage("metadata warning"))
                .build(), PROV);
        assertThat(messageOnly).isEqualTo(new DeliveryResult.Partial(0, "metadata warning", PROV));
    }

    @DisplayName("ProfilesMapper.deliveryResult preserves every present partial_success block")
    @Test
    void profilesDeliveryResultDecisionTable() {
        assertThat(ProfilesMapper.deliveryResult(ExportProfilesServiceResponse.getDefaultInstance(), PROV))
                .isInstanceOf(DeliveryResult.Accepted.class);

        assertThat(ProfilesMapper.deliveryResult(ExportProfilesServiceResponse.newBuilder()
                .setPartialSuccess(ExportProfilesPartialSuccess.getDefaultInstance())
                        .build(), PROV))
                .isEqualTo(new DeliveryResult.Partial(0, "", PROV));

        var withCount = ProfilesMapper.deliveryResult(ExportProfilesServiceResponse.newBuilder()
                .setPartialSuccess(ExportProfilesPartialSuccess.newBuilder()
                        .setRejectedProfiles(4)
                        .setErrorMessage("4 profiles rejected"))
                .build(), PROV);
        assertThat(withCount).isInstanceOf(DeliveryResult.Partial.class);
        var p = (DeliveryResult.Partial) withCount;
        assertThat(p.rejectedItems()).isEqualTo(4);

        var messageOnly = ProfilesMapper.deliveryResult(ExportProfilesServiceResponse.newBuilder()
                .setPartialSuccess(ExportProfilesPartialSuccess.newBuilder()
                        .setErrorMessage("profiles warning"))
                .build(), PROV);
        assertThat(messageOnly).isEqualTo(new DeliveryResult.Partial(0, "profiles warning", PROV));
    }
}
