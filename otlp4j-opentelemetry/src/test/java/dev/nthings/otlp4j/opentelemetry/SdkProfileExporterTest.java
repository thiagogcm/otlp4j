package dev.nthings.otlp4j.opentelemetry;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.AttributeValue;
import dev.nthings.otlp4j.model.Profile;
import dev.nthings.otlp4j.opentelemetry.SdkExportersTest.RecordingInput;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.Value;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.profiles.ProfilesDictionaryCompositor;
import io.opentelemetry.sdk.profiles.data.FunctionData;
import io.opentelemetry.sdk.profiles.data.KeyValueAndUnitData;
import io.opentelemetry.sdk.profiles.data.LineData;
import io.opentelemetry.sdk.profiles.data.LinkData;
import io.opentelemetry.sdk.profiles.data.LocationData;
import io.opentelemetry.sdk.profiles.data.MappingData;
import io.opentelemetry.sdk.profiles.data.ProfileData;
import io.opentelemetry.sdk.profiles.data.ProfilesDictionaryData;
import io.opentelemetry.sdk.profiles.data.SampleData;
import io.opentelemetry.sdk.profiles.data.StackData;
import io.opentelemetry.sdk.profiles.data.ValueTypeData;
import io.opentelemetry.sdk.resources.Resource;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("SDK profile exporter")
final class SdkProfileExporterTest {

    private static final String PROFILE_ID = "0102030405060708090a0b0c0d0e0f10";
    private static final String SPAN_ID = "0102030405060708";
    private static final Resource RESOURCE =
            Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), "checkout"));
    private static final InstrumentationScopeInfo SCOPE = InstrumentationScopeInfo.create("profiler");

    @DisplayName("each profile resolves against its own dictionary, unlike the SDK's own exporter")
    @Test
    void profilesWithDifferentDictionariesExportTogether() {
        var input = new RecordingInput<>(SignalDescriptor.PROFILES);
        var exporter = SdkProfileExporters.profiles(input);

        var result = exporter.export(List.of(profile("reserveStock"), profile("chargeCard")));

        assertThat(result.isSuccess()).isTrue();
        var batch = input.batches.getFirst();
        assertThat(batch.resourceProfiles()).singleElement().satisfies(group -> {
            assertThat(group.resource().attributes().getString("service.name")).isEqualTo("checkout");
            assertThat(group.scopeProfiles()).singleElement().satisfies(scope ->
                    assertThat(scope.scope().name()).isEqualTo("profiler"));
        });
        assertThat(batch.profiles())
                .extracting(profile -> profile.samples().getFirst().stack().locations().getFirst()
                        .lines().getFirst().function().name())
                .containsExactly("reserveStock", "chargeCard");
    }

    @DisplayName("every SDK field reaches the domain profile as a value")
    @Test
    void mapsEveryField() {
        var input = new RecordingInput<>(SignalDescriptor.PROFILES);

        SdkProfileExporters.profiles(input).export(List.of(profile("handle")));

        var profile = input.batches.getFirst().profiles().getFirst();
        assertThat(profile.profileId()).isEqualTo(PROFILE_ID);
        assertThat(profile.epochNanos()).isEqualTo(100L);
        assertThat(profile.durationNanos()).isEqualTo(50L);
        assertThat(profile.sampleType()).isEqualTo(new Profile.ValueType("cpu", "nanoseconds"));
        assertThat(profile.periodType()).isEqualTo(new Profile.ValueType("wall", "milliseconds"));
        assertThat(profile.period()).isEqualTo(10L);
        assertThat(profile.attributes().get("allocated")).isEqualTo(AttributeValue.of(128L));
        assertThat(profile.attributeUnits()).containsExactly(Map.entry("allocated", "By"));
        assertThat(profile.droppedAttributesCount()).isEqualTo(4);
        assertThat(profile.originalPayloadFormat()).isEqualTo("pprof");
        assertThat(profile.originalPayload()).containsExactly(1, 2);

        var sample = profile.samples().getFirst();
        assertThat(sample.values()).containsExactly(30L);
        assertThat(sample.timestamps()).containsExactly(7L);
        assertThat(sample.link()).isEqualTo(new Profile.Link(PROFILE_ID, SPAN_ID));
        assertThat(sample.attributeUnits()).containsExactly(Map.entry("allocated", "By"));

        var location = sample.stack().locations().getFirst();
        assertThat(location.address()).isEqualTo(0x1234L);
        assertThat(location.mapping()).isEqualTo(Profile.Mapping.builder()
                .memoryStart(1L)
                .memoryLimit(2L)
                .fileOffset(3L)
                .filename("/bin/app")
                .attributes(profile.attributes())
                .attributeUnits(Map.of("allocated", "By"))
                .build());
        assertThat(location.lines()).containsExactly(new Profile.Line(
                new Profile.Function("handle", "_Z6handlev", "app.cc", 40L), 12L, 3L));
        assertThat(location.attributes().get("allocated")).isEqualTo(AttributeValue.of(128L));
    }

    @DisplayName("null references resolve to the zero values")
    @Test
    void nullReferencesResolveToZeroValues() {
        var input = new RecordingInput<>(SignalDescriptor.PROFILES);

        SdkProfileExporters.profiles(input).export(List.of(
                bareProfile(SampleData.create(0, List.of(), 0, List.of(1L), List.of()), List.of())));

        var profile = input.batches.getFirst().profiles().getFirst();
        assertThat(profile.sampleType()).isEqualTo(Profile.ValueType.EMPTY);
        assertThat(profile.originalPayloadFormat()).isEmpty();
        assertThat(profile.samples().getFirst().stack()).isEqualTo(Profile.Stack.EMPTY);
        assertThat(profile.samples().getFirst().link()).isEqualTo(Profile.Link.NONE);
    }

    @DisplayName("a reference outside its table fails the export instead of dropping the sample")
    @Test
    void aDanglingReferenceFailsTheExport() {
        var input = new RecordingInput<>(SignalDescriptor.PROFILES);

        var result = SdkProfileExporters.profiles(input).export(List.of(
                bareProfile(SampleData.create(7, List.of(), 0, List.of(1L), List.of()), List.of())));

        assertThat(result.join(1, TimeUnit.SECONDS).isSuccess()).isFalse();
        assertThat(input.batches).isEmpty();
    }

    @DisplayName("an empty export succeeds without reaching the input, until the exporter shuts down")
    @Test
    void anEmptyExportSubmitsNothing() {
        var input = new RecordingInput<>(SignalDescriptor.PROFILES);
        var exporter = SdkProfileExporters.profiles(input);

        assertThat(exporter.export(List.of()).isSuccess()).isTrue();
        assertThat(input.batches).isEmpty();

        assertThat(exporter.shutdown().join(1, TimeUnit.SECONDS).isSuccess()).isTrue();
        assertThat(exporter.export(List.of()).isSuccess()).isFalse();
    }

    @DisplayName("flush and shutdown reach the borrowed input")
    @Test
    void flushAndShutdownReachTheInput() {
        var input = new RecordingInput<>(SignalDescriptor.PROFILES);
        var exporter = SdkProfileExporters.profiles(input);

        input.nextFlush = CompletableFuture.completedFuture(new FlushReport(1, 0, 0, null));
        assertThat(exporter.flush().join(1, TimeUnit.SECONDS).isSuccess()).isFalse();
        input.nextFlush = null;
        assertThat(exporter.flush().join(1, TimeUnit.SECONDS).isSuccess()).isTrue();
        assertThat(exporter.shutdown().join(1, TimeUnit.SECONDS).isSuccess()).isTrue();

        assertThat(input.flushCalls).isEqualTo(3);
    }

    @DisplayName("exporting leaves the SDK's payload buffer unread")
    @Test
    void theOriginalPayloadBufferIsNotConsumed() {
        var input = new RecordingInput<>(SignalDescriptor.PROFILES);
        var profile = profile("handle");

        SdkProfileExporters.profiles(input).export(List.of(profile));

        assertThat(profile.getOriginalPayload().remaining()).isEqualTo(2);
    }

    @DisplayName("a reference to the zero attribute is skipped")
    @Test
    void aNullAttributeReferenceIsSkipped() {
        var input = new RecordingInput<>(SignalDescriptor.PROFILES);

        SdkProfileExporters.profiles(input).export(List.of(
                bareProfile(SampleData.create(0, List.of(0), 0, List.of(1L), List.of()), List.of(0))));

        var profile = input.batches.getFirst().profiles().getFirst();
        assertThat(profile.attributes().isEmpty()).isTrue();
        assertThat(profile.samples().getFirst().attributes().isEmpty()).isTrue();
    }

    /// A profile over an empty dictionary, holding `sample` and nothing else.
    private static ProfileData bareProfile(SampleData sample, List<Integer> attributeIndices) {
        var type = ValueTypeData.create(0, 0);
        return ProfileData.create(
                RESOURCE,
                SCOPE,
                new ProfilesDictionaryCompositor().getProfileDictionaryData(),
                type,
                List.of(sample),
                0L,
                0L,
                type,
                0L,
                PROFILE_ID,
                0,
                null,
                ByteBuffer.allocate(0),
                attributeIndices);
    }

    /// A one-sample profile with its own dictionary, whose frame calls `function`.
    private static ProfileData profile(String function) {
        var dictionary = new ProfilesDictionaryCompositor();
        var functionIndex = dictionary.putIfAbsent(FunctionData.create(
                dictionary.putIfAbsent(function),
                dictionary.putIfAbsent("_Z6handlev"),
                dictionary.putIfAbsent("app.cc"),
                40L));
        var attribute = dictionary.putIfAbsent(KeyValueAndUnitData.create(
                dictionary.putIfAbsent("allocated"), Value.of(128L), dictionary.putIfAbsent("By")));
        var mapping = dictionary.putIfAbsent(
                MappingData.create(1L, 2L, 3L, dictionary.putIfAbsent("/bin/app"), List.of(attribute)));
        var location = dictionary.putIfAbsent(LocationData.create(
                mapping, 0x1234L, List.of(LineData.create(functionIndex, 12L, 3L)), List.of(attribute)));
        var stack = dictionary.putIfAbsent(StackData.create(List.of(location)));
        var link = dictionary.putIfAbsent(LinkData.create(PROFILE_ID, SPAN_ID));
        var sample = SampleData.create(stack, List.of(attribute), link, List.of(30L), List.of(7L));
        ProfilesDictionaryData data = dictionary.getProfileDictionaryData();
        return ProfileData.create(
                RESOURCE,
                SCOPE,
                data,
                ValueTypeData.create(dictionary.putIfAbsent("cpu"), dictionary.putIfAbsent("nanoseconds")),
                List.of(sample),
                100L,
                50L,
                ValueTypeData.create(dictionary.putIfAbsent("wall"), dictionary.putIfAbsent("milliseconds")),
                10L,
                PROFILE_ID,
                5,
                "pprof",
                ByteBuffer.wrap(new byte[] {1, 2}),
                List.of(attribute));
    }
}
