package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.MetricsData;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/// Randomized round-trip property tests for [MetricsMapper].
///
/// Both mapper invariants from the design are pinned across many generated metrics payloads,
/// covering all five point kinds (gauge, sum, histogram, exponential histogram, summary) plus the
/// empty `DATA_NOT_SET` form: domain to proto to domain, and proto to domain to proto. Each
/// invariant runs at least two hundred generated cases from a seeded generator. A failing case
/// carries its seed in the assertion description, so the exact counterexample can be reproduced by
/// calling [ModelGen#seeded(long)] with that seed again.
@DisplayName("MetricsMapper property round-trips")
class MetricsCodecPropertyTest {

    private static final long DOMAIN_BASE_SEED = 300_000L;
    private static final long PROTO_BASE_SEED = 400_000L;
    private static final int CASE_COUNT = 200;

    static Stream<ModelGen.Case<MetricsData>> domainRoundTripCases() {
        return IntStream.range(0, CASE_COUNT).mapToObj(i -> {
            var seed = DOMAIN_BASE_SEED + i;
            return new ModelGen.Case<>(seed, ModelGen.metricsData(ModelGen.seeded(seed)));
        });
    }

    static Stream<ModelGen.Case<ExportMetricsServiceRequest>> protoRoundTripCases() {
        return IntStream.range(0, CASE_COUNT).mapToObj(i -> {
            var seed = PROTO_BASE_SEED + i;
            return new ModelGen.Case<>(seed, ModelGen.protoMetricsRequest(ModelGen.seeded(seed)));
        });
    }

    @DisplayName("domain to proto to domain preserves a randomly generated MetricsData")
    @ParameterizedTest
    @MethodSource("domainRoundTripCases")
    void domainToProtoToDomain(ModelGen.Case<MetricsData> testCase) {
        var roundTripped = MetricsMapper.toDomain(MetricsMapper.toProto(testCase.value()));

        assertThat(roundTripped)
                .as("seed=%d: toDomain(toProto(x)) must equal x", testCase.seed())
                .isEqualTo(testCase.value());
    }

    @DisplayName("proto to domain to proto preserves a randomly generated wire request")
    @ParameterizedTest
    @MethodSource("protoRoundTripCases")
    void protoToDomainToProto(ModelGen.Case<ExportMetricsServiceRequest> testCase) {
        var roundTripped = MetricsMapper.toProto(MetricsMapper.toDomain(testCase.value()));

        // Compared on the wire bytes rather than proto equals so a mismatch reports a byte diff
        // instead of tripping protobuf's reflective toString under JPMS.
        assertThat(roundTripped.toByteArray())
                .as("seed=%d: toProto(toDomain(r)) must equal r on the wire", testCase.seed())
                .isEqualTo(testCase.value().toByteArray());
    }
}
