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

/// Randomized round-trip property tests for [ProfilesMapper].
///
/// Profiles are not fully bijective (see the design's section 4.3 and the existing
/// `ProfilesRoundTripTest`), so this class pins the two narrower invariants the mapper actually
/// supports instead of a general domain round trip:
///
/// - A scalar-only profile's modeled fields survive a domain to proto to domain round trip, with
///   `sampleCount` generated at zero because the mapper's scalar rebuild path never emits samples
///   and so can never reconstruct a nonzero sample count.
/// - An arbitrary rich profile payload, with real samples and a shared dictionary, survives a proto
///   to domain to proto round trip byte-identically through the opaque passthrough path.
///
/// Each invariant runs at least two hundred generated cases from a seeded generator. A failing case
/// carries its seed in the assertion description, so the exact counterexample can be reproduced by
/// calling [ModelGen#seeded(long)] with that seed again.
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

    @DisplayName("a scalar-only profile's modeled fields survive domain to proto to domain, with "
            + "sampleCount pinned at zero")
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
        // The mapper cannot reconstruct sampleCount from scalars alone: toDomain always derives it
        // from the samples list of the rebuilt proto, which the scalar path never populates. Pinned
        // here as an explicit fact rather than an accidental pass on a fixed-at-zero input.
        assertThat(received.sampleCount())
                .as("seed=%d: a scalar-rebuilt profile always reads sampleCount back as zero", testCase.seed())
                .isZero();
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
