package dev.nthings.otlp4j.codec;

import dev.nthings.otlp4j.testing.TransportFixtures;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import dev.nthings.otlp4j.model.ProfilesData;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.profiles.v1development.KeyValueAndUnit;
import io.opentelemetry.proto.profiles.v1development.Profile;
import io.opentelemetry.proto.profiles.v1development.ProfilesDictionary;
import io.opentelemetry.proto.profiles.v1development.ResourceProfiles;
import io.opentelemetry.proto.profiles.v1development.Sample;
import io.opentelemetry.proto.profiles.v1development.ScopeProfiles;
import io.opentelemetry.proto.profiles.v1development.ValueType;
import io.opentelemetry.proto.resource.v1.Resource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Proves profiles forward losslessly: a rich proto export request (samples, sample/period types,
/// original payload, and a shared dictionary) survives proto -> domain -> proto byte-identically,
/// while the domain side still exposes scalar metadata for inspection.
@DisplayName("Profiles lossless passthrough")
class ProfilesRoundTripTest {

    @DisplayName("a rich profiles request round-trips byte-identically while exposing scalar metadata")
    @Test
    void richProfilesRequestRoundTripsLossless() {
        var profile = Profile.newBuilder()
                .setSampleType(ValueType.newBuilder().setTypeStrindex(1).setUnitStrindex(2))
                .addSamples(Sample.newBuilder()
                        .setStackIndex(3)
                        .addValues(10L)
                        .addValues(20L)
                        .addAttributeIndices(1))
                .setTimeUnixNano(1_700_000_000_000_000_000L)
                .setDurationNano(5_000_000L)
                .setPeriodType(ValueType.newBuilder().setTypeStrindex(2).setUnitStrindex(1))
                .setPeriod(99L)
                .setProfileId(ByteString.copyFrom(new byte[] {
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
                }))
                .setDroppedAttributesCount(4)
                .setOriginalPayloadFormat("pprof")
                .setOriginalPayload(ByteString.copyFromUtf8("raw-source-bytes"))
                .addAttributeIndices(1)
                .build();

        var dictionary = ProfilesDictionary.newBuilder()
                .addStringTable("")
                .addStringTable("cpu")
                .addStringTable("nanoseconds")
                .addAttributeTable(KeyValueAndUnit.newBuilder()
                        .setKeyStrindex(1)
                        .setValue(AnyValue.newBuilder().setStringValue("value")))
                .build();

        var request = ExportProfilesServiceRequest.newBuilder()
                .addResourceProfiles(ResourceProfiles.newBuilder()
                        .setResource(Resource.newBuilder()
                                .addAttributes(KeyValue.newBuilder()
                                        .setKey("service.name")
                                        .setValue(AnyValue.newBuilder().setStringValue("checkout"))))
                        .setSchemaUrl("https://schema/resource")
                        .addScopeProfiles(ScopeProfiles.newBuilder()
                                .setScope(InstrumentationScope.newBuilder()
                                        .setName("otlp4j-test")
                                        .setVersion("1.0.0"))
                                .setSchemaUrl("https://schema/scope")
                                .addProfiles(profile)))
                .setDictionary(dictionary)
                .build();

        var domain = ProfilesMapper.toDomain(request);

        // Inspection still works: scalar metadata is surfaced and the raw payload is captured.
        var modeled = domain.profiles().getFirst();
        assertThat(modeled.profileId()).isEqualTo("0102030405060708090a0b0c0d0e0f10");
        assertThat(modeled.sampleCount()).isEqualTo(1);
        assertThat(modeled.timeUnixNano()).isEqualTo(1_700_000_000_000_000_000L);
        assertThat(modeled.originalPayloadFormat()).isEqualTo("pprof");
        assertThat(modeled.rawProfile()).isNotEmpty();
        assertThat(domain.dictionary()).isNotEmpty();

        // Lossless: the round-tripped request serializes byte-identically, so the samples, sample and
        // period types, original payload, dictionary, and the resource/scope wrapper all survived.
        // (Compared on the wire bytes rather than proto equals so a mismatch reports a byte diff
        // instead of tripping protobuf's reflective toString under JPMS.)
        assertThat(ProfilesMapper.toProto(domain).toByteArray()).isEqualTo(request.toByteArray());
    }

    @DisplayName("a scalar-only domain profile maps to proto carrying its scalar metadata")
    @Test
    void scalarOnlyProfileMapsToProtoMetadata() {
        // An empty rawProfile means the mapper rebuilds the proto from the modeled scalar fields.
        var domain = TransportFixtures.profiles(new ProfilesData.Profile(
                "aabbccddeeff00112233445566778899", 111L, 222L, 333L, 0, 7, "jfr", new byte[0]));

        var proto = ProfilesMapper.toProto(domain);

        var profile = proto.getResourceProfiles(0).getScopeProfiles(0).getProfiles(0);
        assertThat(profile.getProfileId().toByteArray())
                .isEqualTo(new byte[] {
                    (byte) 0xaa, (byte) 0xbb, (byte) 0xcc, (byte) 0xdd,
                    (byte) 0xee, (byte) 0xff, 0, 0x11, 0x22, 0x33,
                    0x44, 0x55, 0x66, 0x77, (byte) 0x88, (byte) 0x99
                });
        assertThat(profile.getTimeUnixNano()).isEqualTo(111L);
        assertThat(profile.getDurationNano()).isEqualTo(222L);
        assertThat(profile.getPeriod()).isEqualTo(333L);
        assertThat(profile.getDroppedAttributesCount()).isEqualTo(7);
        assertThat(profile.getOriginalPayloadFormat()).isEqualTo("jfr");
        assertThat(proto.hasDictionary()).isFalse();
    }
}
