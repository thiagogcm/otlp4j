package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.testing.TransportFixtures;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/// Seeded round trips expose telemetry loss and make failures reproducible.
@DisplayName("ProfilesMapper property round-trips")
class ProfilesCodecPropertyTest {

    private static final long SCALAR_BASE_SEED = 700_000L;
    private static final long RICH_BASE_SEED = 800_000L;
    private static final int CASE_COUNT = 200;

    static Stream<ModelGen.Case<ProfilesData.Profile>> scalarProfileCases() {
        return IntStream.range(0, CASE_COUNT).mapToObj(i -> {
            var seed = SCALAR_BASE_SEED + i;
            return new ModelGen.Case<>(seed, ModelGen.scalarProfile(ModelGen.seeded(seed)));
        });
    }

    static Stream<ModelGen.Case<ExportProfilesServiceRequest>> richProfileRequestCases() {
        return IntStream.range(0, CASE_COUNT).mapToObj(i -> {
            var seed = RICH_BASE_SEED + i;
            return new ModelGen.Case<>(seed, ModelGen.protoProfilesRequestRich(ModelGen.seeded(seed)));
        });
    }

    @DisplayName("a scalar-only profile's modeled fields survive domain to proto to domain, without captured sample bytes")
    @ParameterizedTest
    @MethodSource("scalarProfileCases")
    void scalarFieldsSurviveRoundTrip(ModelGen.Case<ProfilesData.Profile> testCase) {
        var profile = testCase.value();
        var sent = TransportFixtures.profiles(profile);

        var roundTripped = ProfilesMapper.toDomain(ProfilesMapper.toProto(sent));

        var received = roundTripped.profiles().getFirst();
        assertThat(received.profileId())
                .as("seed=%d: profileId must survive the round trip", testCase.seed())
                .isEqualTo(profile.profileId());
        assertThat(received.timeUnixNano())
                .as("seed=%d: timeUnixNano must survive the round trip", testCase.seed())
                .isEqualTo(profile.timeUnixNano());
        assertThat(received.durationNanos())
                .as("seed=%d: durationNanos must survive the round trip", testCase.seed())
                .isEqualTo(profile.durationNanos());
        assertThat(received.period())
                .as("seed=%d: period must survive the round trip", testCase.seed())
                .isEqualTo(profile.period());
        assertThat(received.droppedAttributesCount())
                .as("seed=%d: droppedAttributesCount must survive the round trip", testCase.seed())
                .isEqualTo(profile.droppedAttributesCount());
        assertThat(received.originalPayloadFormat())
                .as("seed=%d: originalPayloadFormat must survive the round trip", testCase.seed())
                .isEqualTo(profile.originalPayloadFormat());
    }

    @DisplayName("an arbitrary rich profile payload with samples and a dictionary survives proto to "
            + "domain to proto byte-identically")
    @ParameterizedTest
    @MethodSource("richProfileRequestCases")
    void richProfilePayloadSurvivesLosslessRoundTrip(ModelGen.Case<ExportProfilesServiceRequest> testCase) {
        var proto = testCase.value();

        var domain = ProfilesMapper.toDomain(proto);
        var roundTripped = ProfilesMapper.toProto(domain);

        // Compared on the wire bytes rather than proto equals so a mismatch reports a byte diff
        // instead of tripping protobuf's reflective toString under JPMS.
        assertThat(roundTripped.toByteArray())
                .as("seed=%d: an arbitrary rich profile payload must survive the opaque passthrough "
                        + "byte-identically", testCase.seed())
                .isEqualTo(proto.toByteArray());
    }
}
