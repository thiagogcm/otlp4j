package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.Profile;
import dev.nthings.otlp4j.testing.TransportFixtures;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest;
import java.util.HashSet;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/// Seeded round trips expose telemetry loss and make failures reproducible.
@DisplayName("ProfilesMapper property round-trips")
class ProfilesCodecPropertyTest {

    private static final long DOMAIN_BASE_SEED = 700_000L;
    private static final long PROTO_BASE_SEED = 800_000L;
    private static final int CASE_COUNT = 200;

    static Stream<ModelGen.Case<Profile>> profileCases() {
        return IntStream.range(0, CASE_COUNT).mapToObj(i -> {
            var seed = DOMAIN_BASE_SEED + i;
            return new ModelGen.Case<>(seed, ModelGen.profile(ModelGen.seeded(seed)));
        });
    }

    static Stream<ModelGen.Case<ExportProfilesServiceRequest>> protoRequestCases() {
        return IntStream.range(0, CASE_COUNT).mapToObj(i -> {
            var seed = PROTO_BASE_SEED + i;
            return new ModelGen.Case<>(seed, ModelGen.protoProfilesRequestRich(ModelGen.seeded(seed)));
        });
    }

    @DisplayName("a profile survives domain to proto to domain")
    @ParameterizedTest
    @MethodSource("profileCases")
    void profileSurvivesRoundTrip(ModelGen.Case<Profile> testCase) {
        var sent = TransportFixtures.profiles(testCase.value());

        assertThat(ProfilesMapper.toDomain(ProfilesMapper.toProto(sent)))
                .as("seed=%d: the profile must survive the round trip", testCase.seed())
                .isEqualTo(sent);
    }

    @DisplayName("a request with an untidy dictionary keeps its meaning through domain and proto, and "
            + "encodes to a dictionary without duplicates")
    @ParameterizedTest
    @MethodSource("protoRequestCases")
    void requestKeepsItsMeaning(ModelGen.Case<ExportProfilesServiceRequest> testCase) {
        var decoded = ProfilesMapper.toDomain(testCase.value());

        var encoded = ProfilesMapper.toProto(decoded);

        assertThat(ProfilesMapper.toDomain(encoded))
                .as("seed=%d: re-encoding must not change the meaning", testCase.seed())
                .isEqualTo(decoded);
        var dictionary = encoded.getDictionary();
        assertThat(dictionary.getStringTableList()).as("seed=%d", testCase.seed()).doesNotHaveDuplicates();
        assertThat(new HashSet<>(dictionary.getFunctionTableList())).hasSize(dictionary.getFunctionTableCount());
        assertThat(new HashSet<>(dictionary.getStackTableList())).hasSize(dictionary.getStackTableCount());
    }

    @DisplayName("encoding a batch twice yields the same bytes")
    @ParameterizedTest
    @MethodSource("profileCases")
    void encodingIsDeterministic(ModelGen.Case<Profile> testCase) {
        var batch = TransportFixtures.profiles(testCase.value());

        assertThat(ProfilesMapper.toProto(ProfilesMapper.toDomain(ProfilesMapper.toProto(batch))).toByteArray())
                .as("seed=%d: a decoded batch must re-encode to the same bytes", testCase.seed())
                .isEqualTo(ProfilesMapper.toProto(batch).toByteArray());
    }
}
