package dev.nthings.otlp4j.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.google.protobuf.ByteString;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Profile;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.testing.Fixtures;
import dev.nthings.otlp4j.testing.TransportFixtures;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import io.opentelemetry.proto.profiles.v1development.Function;
import io.opentelemetry.proto.profiles.v1development.KeyValueAndUnit;
import io.opentelemetry.proto.profiles.v1development.Line;
import io.opentelemetry.proto.profiles.v1development.Link;
import io.opentelemetry.proto.profiles.v1development.Location;
import io.opentelemetry.proto.profiles.v1development.Mapping;
import io.opentelemetry.proto.profiles.v1development.ProfilesDictionary;
import io.opentelemetry.proto.profiles.v1development.ResourceProfiles;
import io.opentelemetry.proto.profiles.v1development.Sample;
import io.opentelemetry.proto.profiles.v1development.ScopeProfiles;
import io.opentelemetry.proto.profiles.v1development.Stack;
import io.opentelemetry.proto.profiles.v1development.ValueType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/// Proves the codec replaces dictionary references with values and back without changing what a
/// profile says, which is the guarantee that lets batches merge and split as plain lists.
@DisplayName("Profiles dictionary resolution")
class ProfilesRoundTripTest {

    private static final String ID = "0102030405060708090a0b0c0d0e0f10";
    private static final byte[] TRACE_ID = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    private static final byte[] SPAN_ID = {1, 2, 3, 4, 5, 6, 7, 8};

    /// A dictionary whose every table has one real entry after its zero value.
    private static ProfilesDictionary.Builder dictionary() {
        return ProfilesDictionary.newBuilder()
                .addAllStringTable(List.of(
                        "", "cpu", "nanoseconds", "memcpy", "__memcpy", "memcpy.c", "/usr/lib/libc.so",
                        "thread.id", "allocated", "By", "resource.key", "resource.value"))
                .addMappingTable(Mapping.getDefaultInstance())
                .addMappingTable(Mapping.newBuilder()
                        .setMemoryStart(0x1000L)
                        .setMemoryLimit(0x2000L)
                        .setFileOffset(0x10L)
                        .setFilenameStrindex(6))
                .addFunctionTable(Function.getDefaultInstance())
                .addFunctionTable(Function.newBuilder()
                        .setNameStrindex(3)
                        .setSystemNameStrindex(4)
                        .setFilenameStrindex(5)
                        .setStartLine(10L))
                .addLocationTable(Location.getDefaultInstance())
                .addLocationTable(Location.newBuilder()
                        .setMappingIndex(1)
                        .setAddress(0x1234L)
                        .addLines(Line.newBuilder().setFunctionIndex(1).setLine(12L).setColumn(3L))
                        .addAttributeIndices(1))
                .addStackTable(Stack.getDefaultInstance())
                .addStackTable(Stack.newBuilder().addLocationIndices(1).addLocationIndices(0))
                .addLinkTable(Link.getDefaultInstance())
                .addLinkTable(Link.newBuilder()
                        .setTraceId(ByteString.copyFrom(TRACE_ID))
                        .setSpanId(ByteString.copyFrom(SPAN_ID)))
                .addAttributeTable(KeyValueAndUnit.getDefaultInstance())
                .addAttributeTable(KeyValueAndUnit.newBuilder()
                        .setKeyStrindex(7)
                        .setValue(AnyValue.newBuilder().setIntValue(42L)))
                .addAttributeTable(KeyValueAndUnit.newBuilder()
                        .setKeyStrindex(8)
                        .setValue(AnyValue.newBuilder().setIntValue(128L))
                        .setUnitStrindex(9));
    }

    private static io.opentelemetry.proto.profiles.v1development.Profile.Builder profile() {
        return io.opentelemetry.proto.profiles.v1development.Profile.newBuilder()
                .setProfileId(ByteString.copyFrom(TRACE_ID))
                .setTimeUnixNano(1_700_000_000_000_000_000L)
                .setDurationNano(5_000_000L)
                .setSampleType(ValueType.newBuilder().setTypeStrindex(1).setUnitStrindex(2))
                .setPeriodType(ValueType.newBuilder().setTypeStrindex(1).setUnitStrindex(2))
                .setPeriod(99L)
                .setDroppedAttributesCount(4)
                .setOriginalPayloadFormat("pprof")
                .setOriginalPayload(ByteString.copyFromUtf8("raw-source-bytes"))
                .addAttributeIndices(1)
                .addSamples(sample());
    }

    private static Sample.Builder sample() {
        return Sample.newBuilder()
                .setStackIndex(1)
                .setLinkIndex(1)
                .addAttributeIndices(2)
                .addValues(10L)
                .addValues(20L)
                .addTimestampsUnixNano(1L)
                .addTimestampsUnixNano(2L);
    }

    private static ExportProfilesServiceRequest request(
            io.opentelemetry.proto.profiles.v1development.Profile.Builder profile, ProfilesDictionary.Builder dictionary) {
        return ExportProfilesServiceRequest.newBuilder()
                .addResourceProfiles(ResourceProfiles.newBuilder()
                        .setSchemaUrl("https://schema/resource")
                        .addScopeProfiles(ScopeProfiles.newBuilder()
                                .setSchemaUrl("https://schema/scope")
                                .addProfiles(profile)))
                .setDictionary(dictionary)
                .build();
    }

    @DisplayName("decoding resolves every reference to its value")
    @Test
    void decodingResolvesEveryReference() {
        var decoded = ProfilesMapper.toDomain(request(profile(), dictionary()));

        var profile = decoded.profiles().getFirst();
        assertThat(profile.profileId()).isEqualTo(ID);
        assertThat(profile.sampleType()).isEqualTo(new Profile.ValueType("cpu", "nanoseconds"));
        assertThat(profile.attributes().get("thread.id")).isEqualTo(AttributeValue.of(42L));
        assertThat(profile.attributeUnits()).isEmpty();
        assertThat(profile.originalPayload()).isEqualTo("raw-source-bytes".getBytes());

        var sample = profile.samples().getFirst();
        assertThat(sample.values()).containsExactly(10L, 20L);
        assertThat(sample.timestamps()).containsExactly(1L, 2L);
        assertThat(sample.link()).isEqualTo(new Profile.Link(ID, "0102030405060708"));
        assertThat(sample.attributes().get("allocated")).isEqualTo(AttributeValue.of(128L));
        assertThat(sample.attributeUnits()).containsExactly(Map.entry("allocated", "By"));

        var frames = sample.stack().locations();
        assertThat(frames).hasSize(2);
        assertThat(frames.get(1)).isEqualTo(Profile.Location.EMPTY);
        var leaf = frames.getFirst();
        assertThat(leaf.address()).isEqualTo(0x1234L);
        assertThat(leaf.mapping().filename()).isEqualTo("/usr/lib/libc.so");
        assertThat(leaf.lines()).containsExactly(new Profile.Line(
                new Profile.Function("memcpy", "__memcpy", "memcpy.c", 10L), 12L, 3L));
        assertThat(leaf.attributes().get("thread.id")).isEqualTo(AttributeValue.of(42L));
    }

    @DisplayName("a decoded request re-encodes to a request with the same meaning")
    @Test
    void reencodingKeepsTheMeaning() {
        var decoded = ProfilesMapper.toDomain(request(profile(), dictionary()));

        assertThat(ProfilesMapper.toDomain(ProfilesMapper.toProto(decoded))).isEqualTo(decoded);
    }

    @DisplayName("every populated field survives domain to proto to domain")
    @Test
    void richBatchRoundTrips() {
        var batch = TransportFixtures.profilesData();

        assertThat(ProfilesMapper.toDomain(ProfilesMapper.toProto(batch))).isEqualTo(batch);
    }

    @DisplayName("unsigned wire values survive bit for bit")
    @Test
    void unsignedValuesRoundTrip() {
        var frame = Profile.Location.builder()
                .mapping(Profile.Mapping.builder().memoryStart(-2L).memoryLimit(-1L).fileOffset(Long.MIN_VALUE).build())
                .address(-1L)
                .build();
        var batch = TransportFixtures.profiles(Profile.builder()
                .durationNanos(-1L)
                .addSample(Profile.Sample.builder()
                        .stack(new Profile.Stack(List.of(frame)))
                        .addTimestamp(-1L)
                        .build())
                .build());

        assertThat(ProfilesMapper.toDomain(ProfilesMapper.toProto(batch))).isEqualTo(batch);
    }

    @DisplayName("an encoded dictionary drops orphans and duplicates and keeps the required zero values")
    @Test
    void encodingBuildsAMinimalDictionary() {
        var withOrphansAndDuplicates = dictionary()
                .addStringTable("orphan")
                .addFunctionTable(Function.newBuilder().setNameStrindex(12))
                .addLocationTable(Location.newBuilder().setAddress(0x9999L))
                .addLocationTable(Location.newBuilder()
                        .setMappingIndex(1)
                        .setAddress(0x1234L)
                        .addLines(Line.newBuilder().setFunctionIndex(1).setLine(12L).setColumn(3L))
                        .addAttributeIndices(1))
                .addStackTable(Stack.newBuilder().addLocationIndices(3).addLocationIndices(0));
        var request = request(
                profile().addSamples(sample().setStackIndex(2).setLinkIndex(0).clearAttributeIndices()),
                withOrphansAndDuplicates);

        var encoded = ProfilesMapper.toProto(ProfilesMapper.toDomain(request)).getDictionary();

        assertThat(encoded.getStackTableCount()).isEqualTo(2);
        assertThat(encoded.getLocationTableCount()).isEqualTo(2);
        assertThat(encoded.getFunctionTableCount()).isEqualTo(2);
        assertThat(encoded.getMappingTableCount()).isEqualTo(2);
        assertThat(encoded.getLinkTableCount()).isEqualTo(2);
        assertThat(encoded.getAttributeTableCount()).isEqualTo(3);
        assertThat(encoded.getStringTableList()).doesNotContain("orphan").doesNotHaveDuplicates();

        assertThat(encoded.getStringTable(0)).isEmpty();
        assertThat(encoded.getStackTable(0)).isEqualTo(Stack.getDefaultInstance());
        assertThat(encoded.getLocationTable(0)).isEqualTo(Location.getDefaultInstance());
        assertThat(encoded.getFunctionTable(0)).isEqualTo(Function.getDefaultInstance());
        assertThat(encoded.getMappingTable(0)).isEqualTo(Mapping.getDefaultInstance());
        assertThat(encoded.getAttributeTable(0)).isEqualTo(KeyValueAndUnit.getDefaultInstance());
        // The proto recommends full-length zero ids here for codecs that insist on id lengths.
        assertThat(encoded.getLinkTable(0).getTraceId().size()).isEqualTo(16);
        assertThat(encoded.getLinkTable(0).getSpanId().size()).isEqualTo(8);
    }

    @DisplayName("a batch without profiles carries no dictionary")
    @Test
    void anEmptyBatchHasNoDictionary() {
        assertThat(ProfilesMapper.toProto(new ProfilesData(List.of())).hasDictionary()).isFalse();
    }

    @DisplayName("merging requests that use one index for different things keeps each profile's meaning")
    @Test
    void mergingConflictingDictionariesKeepsEachProfileIntact() {
        var first = ProfilesMapper.toDomain(request(profile(), dictionary()));
        var shifted = dictionary().setStringTable(3, "free").setFunctionTable(1, Function.newBuilder()
                .setNameStrindex(3)
                .setStartLine(99L));
        var second = ProfilesMapper.toDomain(request(profile(), shifted));
        assertThat(first).isNotEqualTo(second);

        var merged = SignalDescriptor.PROFILES.merge(List.of(first, second));

        var received = ProfilesMapper.toDomain(ProfilesMapper.toProto(merged));
        assertThat(received.profiles()).containsExactly(first.profiles().getFirst(), second.profiles().getFirst());
    }

    @DisplayName("each split part's dictionary holds only what that part refers to")
    @Test
    void splittingPrunesEachPartsDictionary() {
        var batch = TransportFixtures.profiles(Fixtures.profile(ID, "alpha"), Fixtures.profile(ID, "beta"));

        var parts = SignalDescriptor.PROFILES.split(batch, 1);

        assertThat(ProfilesMapper.toProto(parts.get(0)).getDictionary().getStringTableList())
                .contains("alpha")
                .doesNotContain("beta");
        assertThat(ProfilesMapper.toProto(parts.get(1)).getDictionary().getStringTableList())
                .contains("beta")
                .doesNotContain("alpha");
    }

    @DisplayName("resource and scope attributes may refer to the dictionary's strings")
    @Test
    void resourceAndScopeAttributesResolveAgainstTheStringTable() {
        var indirect = KeyValue.newBuilder()
                .setKeyStrindex(10)
                .setValue(AnyValue.newBuilder().setStringValueStrindex(11))
                .build();
        var request = ExportProfilesServiceRequest.newBuilder()
                .addResourceProfiles(ResourceProfiles.newBuilder()
                        .setResource(io.opentelemetry.proto.resource.v1.Resource.newBuilder().addAttributes(indirect))
                        .addScopeProfiles(ScopeProfiles.newBuilder()
                                .setScope(io.opentelemetry.proto.common.v1.InstrumentationScope.newBuilder()
                                        .addAttributes(indirect))
                                .addProfiles(profile())))
                .setDictionary(dictionary())
                .build();
        var expected = Attributes.builder().put("resource.key", "resource.value").build();

        var group = ProfilesMapper.toDomain(request).resourceProfiles().getFirst();

        assertThat(group.resource()).isEqualTo(Resource.of(expected));
        assertThat(group.scopeProfiles().getFirst().scope().attributes()).isEqualTo(expected);
        assertThat(ProfilesMapper.protoResourceAttributes(request)).containsExactly(expected);
    }

    @DisplayName("encoded resource and scope attributes hold their keys and strings in the dictionary")
    @Test
    void resourceAndScopeAttributesEncodeAsReferences() {
        var nested = Attributes.builder()
                .put("service.name", "checkout")
                .put("tags", AttributeValue.of(List.of(AttributeValue.of("a"), AttributeValue.of(1L))))
                .put("labels", AttributeValue.of(Map.of("team", AttributeValue.of("payments"))))
                .build();
        var batch = new ProfilesData(List.of(new ProfilesData.ResourceProfiles(
                Resource.of(nested),
                "",
                List.of(new ProfilesData.ScopeProfiles(
                        new InstrumentationScope("cpu", "1.0", nested, 0), "", List.of(Fixtures.profile(ID)))))));

        var encoded = ProfilesMapper.toProto(batch);

        var strings = encoded.getDictionary().getStringTableList();
        var attributes = encoded.getResourceProfiles(0).getResource().getAttributesList();
        attributes.forEach(ProfilesRoundTripTest::assertHoldsNoInlineString);
        encoded.getResourceProfiles(0).getScopeProfiles(0).getScope().getAttributesList()
                .forEach(ProfilesRoundTripTest::assertHoldsNoInlineString);
        assertThat(strings.get(attributes.get(0).getKeyStrindex())).isEqualTo("service.name");
        assertThat(strings.get(attributes.get(0).getValue().getStringValueStrindex())).isEqualTo("checkout");
        assertThat(ProfilesMapper.toDomain(encoded)).isEqualTo(batch);
        assertThat(ProfilesMapper.protoResourceAttributes(encoded)).containsExactly(nested);
    }

    private static void assertHoldsNoInlineString(KeyValue attribute) {
        assertThat(attribute.getKey()).isEmpty();
        assertHoldsNoInlineString(attribute.getValue());
    }

    private static void assertHoldsNoInlineString(AnyValue value) {
        assertThat(value.getValueCase()).isNotEqualTo(AnyValue.ValueCase.STRING_VALUE);
        value.getArrayValue().getValuesList().forEach(ProfilesRoundTripTest::assertHoldsNoInlineString);
        value.getKvlistValue().getValuesList().forEach(ProfilesRoundTripTest::assertHoldsNoInlineString);
    }

    @DisplayName("a resource without profiles still resolves its attributes")
    @Test
    void aBatchWithoutProfilesKeepsItsResourceAttributes() {
        var batch = new ProfilesData(List.of(
                new ProfilesData.ResourceProfiles(Fixtures.checkoutResource(), "", List.of())));

        assertThat(ProfilesMapper.toDomain(ProfilesMapper.toProto(batch))).isEqualTo(batch);
    }

    @DisplayName("a string many resources repeat by reference stays one string when encoded")
    @Test
    void repeatedResourceStringsDoNotAmplifyEncoding() {
        var dictionary = ProfilesDictionary.newBuilder().addStringTable("").addStringTable("x".repeat(64 * 1024));
        var request = ExportProfilesServiceRequest.newBuilder().setDictionary(dictionary);
        for (var i = 0; i < 1_000; i++) {
            request.addResourceProfiles(ResourceProfiles.newBuilder()
                    .setResource(io.opentelemetry.proto.resource.v1.Resource.newBuilder().addAttributes(
                            KeyValue.newBuilder().setKey("k").setValue(AnyValue.newBuilder().setStringValueStrindex(1)))));
        }
        var received = request.build();

        var encoded = ProfilesMapper.toProto(ProfilesMapper.toDomain(received));

        assertThat(encoded.getSerializedSize()).isLessThan(2 * received.getSerializedSize());
    }

    @DisplayName("attribute values that references would expand far past their dictionary are refused")
    @Test
    void anExpandingAttributeValueFailsTheDecode() {
        var array = io.opentelemetry.proto.common.v1.ArrayValue.newBuilder();
        for (var i = 0; i < 1_000; i++) {
            array.addValues(AnyValue.newBuilder().setStringValueStrindex(12));
        }
        var dictionary = dictionary()
                .addStringTable("x".repeat(64 * 1024))
                .setAttributeTable(1, KeyValueAndUnit.newBuilder()
                        .setKeyStrindex(7)
                        .setValue(AnyValue.newBuilder().setArrayValue(array)));

        assertThatIllegalArgumentException()
                .isThrownBy(() -> ProfilesMapper.toDomain(request(profile(), dictionary)))
                .withMessageContaining("expand");
    }

    @DisplayName("expansion is measured in encoded bytes, not characters")
    @Test
    void multiByteStringsCountByTheirEncodedSize() {
        var array = io.opentelemetry.proto.common.v1.ArrayValue.newBuilder();
        for (var i = 0; i < 8; i++) {
            array.addValues(AnyValue.newBuilder().setStringValueStrindex(12));
        }
        var threeBytesEach = dictionary()
                .addStringTable("\u20ac".repeat(64 * 1024))
                .setAttributeTable(1, KeyValueAndUnit.newBuilder()
                        .setKeyStrindex(7)
                        .setValue(AnyValue.newBuilder().setArrayValue(array)));

        assertThatIllegalArgumentException()
                .isThrownBy(() -> ProfilesMapper.toDomain(request(profile(), threeBytesEach)))
                .withMessageContaining("expand");
    }

    @DisplayName("the room every request gets stays small next to what a reference can repeat")
    @Test
    void theExpansionFloorIsSmall() {
        var array = io.opentelemetry.proto.common.v1.ArrayValue.newBuilder();
        for (var i = 0; i < 100; i++) {
            array.addValues(AnyValue.newBuilder().setStringValueStrindex(12));
        }
        var dictionary = dictionary()
                .addStringTable("x".repeat(1024))
                .setAttributeTable(1, KeyValueAndUnit.newBuilder()
                        .setKeyStrindex(7)
                        .setValue(AnyValue.newBuilder().setArrayValue(array)));

        assertThatIllegalArgumentException()
                .isThrownBy(() -> ProfilesMapper.toDomain(request(profile(), dictionary)))
                .withMessageContaining("expand");
    }

    @DisplayName("a small dictionary may repeat a string past the expansion ratio")
    @Test
    void aSmallRepeatedValueIsNotRefused() {
        var repeated = "x".repeat(200);
        var dictionary = ProfilesDictionary.newBuilder().addStringTable("").addStringTable(repeated);
        var profile = io.opentelemetry.proto.profiles.v1development.Profile.newBuilder();
        dictionary.addAttributeTable(KeyValueAndUnit.getDefaultInstance());
        for (var i = 1; i <= 12; i++) {
            dictionary.addStringTable("key-" + i);
            dictionary.addAttributeTable(KeyValueAndUnit.newBuilder()
                    .setKeyStrindex(dictionary.getStringTableCount() - 1)
                    .setValue(AnyValue.newBuilder().setStringValueStrindex(1)));
            profile.addAttributeIndices(i);
        }

        var decoded = ProfilesMapper.toDomain(request(profile, dictionary)).profiles().getFirst();

        assertThat(decoded.attributes().size()).isEqualTo(12);
        assertThat(decoded.attributes().getString("key-1")).isEqualTo(repeated);
    }

    @DisplayName("samples repeating an attribute list share one unit map")
    @Test
    void repeatedAttributeListsShareOneUnitMap() {
        var decoded = ProfilesMapper.toDomain(request(profile().addSamples(sample()), dictionary()));

        var samples = decoded.profiles().getFirst().samples();

        assertThat(samples.get(1).attributeUnits()).isNotEmpty().isSameAs(samples.get(0).attributeUnits());
    }

    @DisplayName("attributes that differ only by unit keep their own units")
    @Test
    void attributesDifferingByUnitStayApart() {
        var batch = TransportFixtures.profiles(Profile.builder()
                .attributes(Attributes.builder().put("size", 1L).build())
                .attributeUnits(Map.of("size", "By"))
                .addSample(Profile.Sample.builder()
                        .attributes(Attributes.builder().put("size", 1L).build())
                        .attributeUnits(Map.of("size", "KiBy"))
                        .addValue(1L)
                        .build())
                .build());

        assertThat(ProfilesMapper.toDomain(ProfilesMapper.toProto(batch))).isEqualTo(batch);
    }

    @DisplayName("a non-empty first string is read like any other entry")
    @Test
    void aNonEmptyFirstStringIsRead() {
        var dictionary = dictionary().setStringTable(0, "first");
        var decoded = ProfilesMapper.toDomain(request(profile().setSampleType(ValueType.getDefaultInstance()), dictionary));

        assertThat(decoded.profiles().getFirst().sampleType()).isEqualTo(new Profile.ValueType("first", "first"));
    }

    @DisplayName("an attribute value may refer to strings at any depth")
    @Test
    void attributeValuesResolveNestedStringReferences() {
        var nested = AnyValue.newBuilder().setKvlistValue(KeyValueList.newBuilder().addValues(KeyValue.newBuilder()
                .setKeyStrindex(10)
                .setValue(AnyValue.newBuilder().setStringValueStrindex(11))));
        var dictionary = dictionary()
                .addAttributeTable(KeyValueAndUnit.newBuilder().setKeyStrindex(7).setValue(nested));

        var decoded = ProfilesMapper.toDomain(request(profile().clearAttributeIndices().addAttributeIndices(3), dictionary));

        assertThat(decoded.profiles().getFirst().attributes().get("thread.id"))
                .isEqualTo(AttributeValue.of(Map.of("resource.key", AttributeValue.of("resource.value"))));
    }

    @DisplayName("routing reads an unresolvable reference as absent so forwarding does not fail on it")
    @Test
    void routingToleratesAnUnresolvableReference() {
        var pastTheEnd = KeyValue.newBuilder()
                .setKey("service.name")
                .setValue(AnyValue.newBuilder().setStringValueStrindex(500))
                .build();
        var negative = KeyValue.newBuilder()
                .setKey("host.name")
                .setValue(AnyValue.newBuilder().setStringValueStrindex(-1))
                .build();
        var request = ExportProfilesServiceRequest.newBuilder()
                .addResourceProfiles(ResourceProfiles.newBuilder()
                        .setResource(io.opentelemetry.proto.resource.v1.Resource.newBuilder()
                                .addAttributes(pastTheEnd)
                                .addAttributes(negative)))
                .build();

        assertThat(ProfilesMapper.protoResourceAttributes(request)).containsExactly(Attributes.builder()
                .put("service.name", AttributeValue.empty())
                .put("host.name", AttributeValue.empty())
                .build());
    }

    @DisplayName("a request without a dictionary decodes while nothing refers into it")
    @Test
    void aMissingDictionaryResolvesNullReferences() {
        var request = ExportProfilesServiceRequest.newBuilder()
                .addResourceProfiles(ResourceProfiles.newBuilder().addScopeProfiles(ScopeProfiles.newBuilder()
                        .addProfiles(io.opentelemetry.proto.profiles.v1development.Profile.newBuilder()
                                .addSamples(Sample.newBuilder().addValues(1L)))))
                .build();

        var sample = ProfilesMapper.toDomain(request).profiles().getFirst().samples().getFirst();

        assertThat(sample.stack()).isEqualTo(Profile.Stack.EMPTY);
        assertThat(sample.link()).isEqualTo(Profile.Link.NONE);
    }

    @DisplayName("an all-zero link is no link, whichever table entry spells it")
    @Test
    void anAllZeroLinkDecodesAsNoLink() {
        var dictionary = dictionary().addLinkTable(Link.newBuilder()
                .setTraceId(ByteString.copyFrom(new byte[16]))
                .setSpanId(ByteString.copyFrom(new byte[8])));
        var sample = ProfilesMapper.toDomain(request(profile().setSamples(0, sample().setLinkIndex(2)), dictionary))
                .profiles().getFirst().samples().getFirst();

        assertThat(sample.link()).isEqualTo(Profile.Link.NONE);
    }

    @DisplayName("an application-built all-zero link encodes as the null reference")
    @Test
    void anApplicationBuiltZeroLinkEncodesAsTheNullReference() {
        var zero = new Profile.Link("0".repeat(32), "0".repeat(16));
        var batch = TransportFixtures.profiles(Profile.builder()
                .addSample(Profile.Sample.builder().link(zero).addValue(1L).build())
                .build());

        var encoded = ProfilesMapper.toProto(batch);

        assertThat(encoded.getDictionary().getLinkTableCount()).isEqualTo(1);
        assertThat(encoded.getResourceProfiles(0).getScopeProfiles(0).getProfiles(0).getSamples(0).getLinkIndex())
                .isZero();
    }

    @DisplayName("samples repeating a stack share one resolved object")
    @Test
    void repeatedReferencesShareOneObject() {
        var decoded = ProfilesMapper.toDomain(request(profile().addSamples(sample()), dictionary()));

        var samples = decoded.profiles().getFirst().samples();

        assertThat(samples.get(1).stack()).isSameAs(samples.get(0).stack());
        assertThat(samples.get(1).attributes()).isSameAs(samples.get(0).attributes());
    }

    @DisplayName("a profile's own attribute keeps its unit")
    @Test
    void profileAttributeUnitsResolve() {
        var decoded = ProfilesMapper.toDomain(request(profile().addAttributeIndices(2), dictionary()));

        assertThat(decoded.profiles().getFirst().attributeUnits()).containsExactly(Map.entry("allocated", "By"));
    }

    @DisplayName("a link with only one id set is still a link")
    @Test
    void aPartlyZeroLinkStaysALink() {
        var dictionary = dictionary().addLinkTable(Link.newBuilder()
                .setTraceId(ByteString.copyFrom(new byte[16]))
                .setSpanId(ByteString.copyFrom(SPAN_ID)));
        var sample = ProfilesMapper.toDomain(request(profile().setSamples(0, sample().setLinkIndex(2)), dictionary))
                .profiles().getFirst().samples().getFirst();

        assertThat(sample.link()).isEqualTo(new Profile.Link("0".repeat(32), "0102030405060708"));
    }

    @DisplayName("a reference to the zero attribute is skipped")
    @Test
    void aNullAttributeReferenceIsSkipped() {
        var decoded = ProfilesMapper.toDomain(
                request(profile().clearAttributeIndices().addAttributeIndices(0).addAttributeIndices(1), dictionary()));

        assertThat(decoded.profiles().getFirst().attributes().keys()).containsExactly("thread.id");
    }

    @DisplayName("the last of two attributes with one key wins, with its unit or lack of one")
    @Test
    void theLastDuplicateKeyWins() {
        var dictionary = dictionary().addAttributeTable(KeyValueAndUnit.newBuilder()
                .setKeyStrindex(8)
                .setValue(AnyValue.newBuilder().setIntValue(7L)));
        var decoded = ProfilesMapper.toDomain(
                request(profile().clearAttributeIndices().addAttributeIndices(2).addAttributeIndices(3), dictionary));

        var profile = decoded.profiles().getFirst();
        assertThat(profile.attributes().get("allocated")).isEqualTo(AttributeValue.of(7L));
        assertThat(profile.attributeUnits()).isEmpty();
    }

    @DisplayName("a sample in a shape the proto forbids is carried as it arrived rather than failing the request")
    @Test
    void aMismatchedSampleSurvives() {
        var mismatched = sample().clearValues().addValues(10L);
        var decoded = ProfilesMapper.toDomain(request(profile().setSamples(0, mismatched), dictionary()));

        var sample = ProfilesMapper.toDomain(ProfilesMapper.toProto(decoded)).profiles().getFirst().samples().getFirst();

        assertThat(sample.values()).containsExactly(10L);
        assertThat(sample.timestamps()).containsExactly(1L, 2L);
    }

    @DisplayName("a producer that broke the zero-entry rule keeps its data")
    @Test
    void aNonZeroFirstEntryIsReadLikeAnyOther() {
        var dictionary = dictionary().setStackTable(0, Stack.newBuilder().addLocationIndices(1));
        var decoded = ProfilesMapper.toDomain(request(profile().setSamples(0, sample().setStackIndex(0)), dictionary));

        var received = ProfilesMapper.toDomain(ProfilesMapper.toProto(decoded));

        assertThat(received.profiles().getFirst().samples().getFirst().stack().locations()).hasSize(1);
    }

    @DisplayName("a value referenced by many attribute lists is resolved once, whatever its size")
    @Test
    void referencesDoNotAmplifyAnAttributeValue() {
        var large = KeyValueAndUnit.newBuilder()
                .setKeyStrindex(7)
                .setValue(AnyValue.newBuilder().setBytesValue(ByteString.copyFrom(new byte[1024])))
                .build();
        var other = KeyValueAndUnit.newBuilder()
                .setKeyStrindex(8)
                .setValue(AnyValue.newBuilder().setIntValue(1L))
                .build();
        var dictionary = dictionary().addAttributeTable(large).addAttributeTable(other);
        var lists = profile()
                .setSamples(0, sample().clearAttributeIndices().addAttributeIndices(3))
                .addSamples(sample().clearAttributeIndices().addAttributeIndices(3).addAttributeIndices(4));

        var samples = ProfilesMapper.toDomain(request(lists, dictionary)).profiles().getFirst().samples();

        assertThat(samples.get(1).attributes().get("thread.id")).isSameAs(samples.get(0).attributes().get("thread.id"));
    }

    @DisplayName("locations that all hash alike encode in time proportional to their number")
    @Test
    void collidingEntriesDoNotDegradeEncoding() {
        var frames = new ArrayList<Profile.Location>();
        for (long k = 1; k <= 50_000; k++) {
            // Long.hashCode folds the two halves together, so every one of these hashes to zero.
            frames.add(Profile.Location.builder().address(k << 32 | k).build());
        }
        var batch = TransportFixtures.profiles(Profile.builder()
                .addSample(Profile.Sample.builder().stack(new Profile.Stack(frames)).addValue(1L).build())
                .build());

        var encoded = assertTimeoutPreemptively(Duration.ofSeconds(10), () -> ProfilesMapper.toProto(batch));

        assertThat(encoded.getDictionary().getLocationTableCount()).isEqualTo(50_001);
    }

    @DisplayName("attribute lists that all hash alike decode in time proportional to their number")
    @Test
    void collidingAttributeListsDoNotDegradeDecoding() {
        var dictionary = dictionary();
        for (var i = 0; i < 40_000; i++) {
            dictionary.addAttributeTable(KeyValueAndUnit.newBuilder().setKeyStrindex(7));
        }
        var lists = listsWithOneHashCode(80_000, 40_000);
        var profile = profile();
        for (var list : lists) {
            profile.addSamples(sample().clearAttributeIndices().addAllAttributeIndices(list));
        }

        assertThat(lists).hasSize(80_000);
        assertThat(lists.stream().map(List::hashCode).distinct()).hasSize(1);
        assertThat(assertTimeoutPreemptively(Duration.ofSeconds(10),
                () -> ProfilesMapper.toDomain(request(profile, dictionary)).profileCount())).isEqualTo(1);
    }

    /// `count` distinct four-element lists of indices below `bound` with one [List#hashCode]. Each
    /// choice of the first three elements leaves one last element that restores the shared hash.
    private static List<List<Integer>> listsWithOneHashCode(int count, int bound) {
        var target = List.of(4, 100, 100, 100).hashCode();
        var lists = new ArrayList<List<Integer>>(count);
        for (var a = 1; a < 8; a++) {
            for (var b = 1; b < 400; b++) {
                for (var c = 1; c < 400 && lists.size() < count; c++) {
                    var d = target - List.of(a, b, c, 0).hashCode();
                    if (d > 0 && d < bound) {
                        lists.add(List.of(a, b, c, d));
                    }
                }
            }
        }
        return lists;
    }

    @DisplayName("attribute keys that all hash alike decode and encode in time proportional to their number")
    @Test
    void collidingAttributeKeysDoNotDegrade() {
        var dictionary = dictionary();
        var profile = profile().clearAttributeIndices();
        var first = dictionary.getAttributeTableCount();
        for (var i = 0; i < 1 << 16; i++) {
            // "Aa" and "BB" hash alike, so every sequence of them hashes alike too.
            var key = new StringBuilder();
            for (var bit = 0; bit < 16; bit++) {
                key.append((i >> bit & 1) == 0 ? "Aa" : "BB");
            }
            dictionary.addStringTable(key.toString());
            dictionary.addAttributeTable(KeyValueAndUnit.newBuilder()
                    .setKeyStrindex(dictionary.getStringTableCount() - 1)
                    .setValue(AnyValue.newBuilder().setIntValue(i))
                    .setUnitStrindex(9));
            profile.addAttributeIndices(first + i);
        }
        var request = request(profile, dictionary);

        var received = assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            var decoded = ProfilesMapper.toDomain(request);
            return ProfilesMapper.toDomain(ProfilesMapper.toProto(decoded));
        });

        assertThat(received.profiles().getFirst().attributeUnits()).hasSize(1 << 16);
    }

    @DisplayName("a malformed entry nothing refers to does not fail the request")
    @Test
    void anUnreferencedMalformedEntryIsIgnored() {
        var dictionary = dictionary().addLinkTable(Link.newBuilder().setTraceId(ByteString.copyFromUtf8("short")));

        assertThat(ProfilesMapper.toDomain(request(profile(), dictionary)).profileCount()).isEqualTo(1);
    }

    static Stream<Arguments> danglingReferences() {
        return Stream.of(
                dangling("stack", p -> p.setSamples(0, sample().setStackIndex(9)), d -> d),
                dangling("negative stack", p -> p.setSamples(0, sample().setStackIndex(-1)), d -> d),
                dangling("link", p -> p.setSamples(0, sample().setLinkIndex(9)), d -> d),
                dangling("sample attribute", p -> p.setSamples(0, sample().addAttributeIndices(9)), d -> d),
                dangling("profile attribute", p -> p.addAttributeIndices(9), d -> d),
                dangling("sample type string", p -> p.setSampleType(ValueType.newBuilder().setTypeStrindex(99)), d -> d),
                dangling("period type string", p -> p.setPeriodType(ValueType.newBuilder().setUnitStrindex(99)), d -> d),
                dangling("location", p -> p, d -> d.setStackTable(1, Stack.newBuilder().addLocationIndices(9))),
                dangling("function", p -> p, d -> d.setLocationTable(1, Location.newBuilder()
                        .addLines(Line.newBuilder().setFunctionIndex(9)))),
                dangling("mapping", p -> p, d -> d.setLocationTable(1, Location.newBuilder().setMappingIndex(9))),
                dangling("function name", p -> p, d -> d.setFunctionTable(1, Function.newBuilder().setNameStrindex(99))),
                dangling("mapping filename", p -> p, d -> d.setMappingTable(1, Mapping.newBuilder().setFilenameStrindex(99))),
                dangling("attribute key", p -> p, d -> d.setAttributeTable(1, KeyValueAndUnit.newBuilder().setKeyStrindex(99))),
                dangling("attribute unit", p -> p, d -> d.setAttributeTable(1, KeyValueAndUnit.newBuilder().setUnitStrindex(99))),
                dangling("attribute string value", p -> p, d -> d.setAttributeTable(1, KeyValueAndUnit.newBuilder()
                        .setValue(AnyValue.newBuilder().setStringValueStrindex(99)))));
    }

    private static Arguments dangling(
            String name,
            UnaryOperator<io.opentelemetry.proto.profiles.v1development.Profile.Builder> profile,
            UnaryOperator<ProfilesDictionary.Builder> dictionary) {
        return Arguments.of(name, profile, dictionary);
    }

    @DisplayName("a reference outside its table fails rather than changing the profile")
    @ParameterizedTest(name = "{0}")
    @MethodSource("danglingReferences")
    void aDanglingReferenceFailsTheDecode(
            String name,
            UnaryOperator<io.opentelemetry.proto.profiles.v1development.Profile.Builder> profile,
            UnaryOperator<ProfilesDictionary.Builder> dictionary) {
        var request = request(profile.apply(profile()), dictionary.apply(dictionary()));

        assertThatIllegalArgumentException().isThrownBy(() -> ProfilesMapper.toDomain(request));
    }

    @DisplayName("a resource attribute referring outside the string table fails the decode")
    @Test
    void aDanglingResourceAttributeFailsTheDecode() {
        var request = ExportProfilesServiceRequest.newBuilder()
                .addResourceProfiles(ResourceProfiles.newBuilder()
                        .setResource(io.opentelemetry.proto.resource.v1.Resource.newBuilder()
                                .addAttributes(KeyValue.newBuilder().setKeyStrindex(500))))
                .build();

        assertThatIllegalArgumentException().isThrownBy(() -> ProfilesMapper.toDomain(request));
    }

    @DisplayName("the scope and resource of a profile survive with their schema URLs")
    @Test
    void groupingSurvives() {
        var batch = new ProfilesData(List.of(new ProfilesData.ResourceProfiles(
                Fixtures.checkoutResource(),
                "https://schema/resource",
                List.of(new ProfilesData.ScopeProfiles(
                        InstrumentationScope.of("cpu", "1.0"),
                        "https://schema/scope",
                        List.of(Fixtures.profile(ID)))))));

        assertThat(ProfilesMapper.toDomain(ProfilesMapper.toProto(batch))).isEqualTo(batch);
    }
}
