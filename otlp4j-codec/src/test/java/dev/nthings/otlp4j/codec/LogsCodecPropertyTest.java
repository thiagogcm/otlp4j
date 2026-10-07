package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.LogsData;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/// Seeded round trips expose telemetry loss and make failures reproducible.
@DisplayName("LogsMapper property round-trips")
class LogsCodecPropertyTest {

    private static final long DOMAIN_BASE_SEED = 500_000L;
    private static final long PROTO_BASE_SEED = 600_000L;
    private static final int CASE_COUNT = 200;

    static Stream<ModelGen.Case<LogsData>> domainRoundTripCases() {
        return IntStream.range(0, CASE_COUNT).mapToObj(i -> {
            var seed = DOMAIN_BASE_SEED + i;
            return new ModelGen.Case<>(seed, ModelGen.logsData(ModelGen.seeded(seed)));
        });
    }

    static Stream<ModelGen.Case<ExportLogsServiceRequest>> protoRoundTripCases() {
        return IntStream.range(0, CASE_COUNT).mapToObj(i -> {
            var seed = PROTO_BASE_SEED + i;
            return new ModelGen.Case<>(seed, ModelGen.protoLogsRequest(ModelGen.seeded(seed)));
        });
    }

    @DisplayName("domain to proto to domain preserves a randomly generated LogsData")
    @ParameterizedTest
    @MethodSource("domainRoundTripCases")
    void domainToProtoToDomain(ModelGen.Case<LogsData> testCase) {
        var roundTripped = LogsMapper.toDomain(LogsMapper.toProto(testCase.value()));

        assertThat(roundTripped)
                .as("seed=%d: toDomain(toProto(x)) must equal x", testCase.seed())
                .isEqualTo(testCase.value());
    }

    @DisplayName("proto to domain to proto preserves a randomly generated wire request")
    @ParameterizedTest
    @MethodSource("protoRoundTripCases")
    void protoToDomainToProto(ModelGen.Case<ExportLogsServiceRequest> testCase) {
        var roundTripped = LogsMapper.toProto(LogsMapper.toDomain(testCase.value()));

        // Compared on the wire bytes rather than proto equals so a mismatch reports a byte diff
        // instead of tripping protobuf's reflective toString under JPMS.
        assertThat(roundTripped.toByteArray())
                .as("seed=%d: toProto(toDomain(r)) must equal r on the wire", testCase.seed())
                .isEqualTo(testCase.value().toByteArray());
    }
}
