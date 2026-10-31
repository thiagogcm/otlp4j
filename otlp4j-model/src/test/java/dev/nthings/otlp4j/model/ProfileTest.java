package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Covers what [Profile] promises beyond a plain record: the invariants the proto states as MUSTs, the
/// value semantics of its payload bytes, and copy-modify through the builders.
@DisplayName("Profile")
class ProfileTest {

    private static final String ID = "0102030405060708090a0b0c0d0e0f10";

    private static Profile profile(byte[] payload) {
        return Profile.builder().profileId(ID).originalPayloadFormat("pprof").originalPayload(payload).build();
    }

    @DisplayName("a sample needs values or timestamps")
    @Test
    void sampleRequiresObservations() {
        assertThatIllegalArgumentException().isThrownBy(() -> Profile.Sample.builder().build());
    }

    @DisplayName("a sample's values and timestamps pair up when both are populated")
    @Test
    void sampleRequiresMatchingSeries() {
        assertThatIllegalArgumentException().isThrownBy(() -> Profile.Sample.builder()
                .addValue(1L)
                .addValue(2L)
                .addTimestamp(10L)
                .build());
    }

    @DisplayName("a sample may hold only values, only timestamps, or a pair of each")
    @Test
    void sampleAcceptsEveryShape() {
        assertThat(Profile.Sample.builder().addValue(10L).build().timestamps()).isEmpty();
        assertThat(Profile.Sample.builder().addTimestamp(1L).addTimestamp(2L).build().values()).isEmpty();
        assertThat(Profile.Sample.builder().addValue(2L).addTimestamp(1L).build().values()).containsExactly(2L);
    }

    @DisplayName("a sample received in a shape the builder refuses is carried as it arrived")
    @Test
    void theRecordKeepsWhatASenderWrote() {
        var sample = new Profile.Sample(
                Profile.Stack.EMPTY, Attributes.empty(), Map.of(), Profile.Link.NONE, List.of(1L), List.of(10L, 20L));

        assertThat(sample.timestamps()).containsExactly(10L, 20L);
    }

    @DisplayName("a transform can rebuild a sample received in a shape the builder would refuse")
    @Test
    void aReceivedSampleSurvivesACopyingTransform() {
        var received = new Profile.Sample(
                Profile.Stack.EMPTY, Attributes.empty(), Map.of(), Profile.Link.NONE, List.of(), List.of());

        var redacted = received.toBuilder().attributes(Attributes.builder().put("env", "prod").build()).build();

        assertThat(redacted.attributes().getString("env")).isEqualTo("prod");
    }

    @DisplayName("removing an attribute takes its unit along")
    @Test
    void aTransformCanDropAnAttributeWithAUnit() {
        var sample = Profile.Sample.builder()
                .attributes(Attributes.builder().put("allocation_size", 128L).put("thread.name", "worker").build())
                .attributeUnits(Map.of("allocation_size", "By"))
                .addValue(1L)
                .build();

        var redacted = sample.toBuilder()
                .attributes(Attributes.builder().put("thread.name", "worker").build())
                .build();

        assertThat(redacted.attributeUnits()).isEmpty();
    }

    @DisplayName("a unit that says nothing is the same as no unit")
    @Test
    void anEmptyUnitIsDropped() {
        var location = Profile.Location.builder()
                .attributes(Attributes.builder().put("allocation_size", 128L).build())
                .attributeUnits(Map.of("allocation_size", ""))
                .build();

        assertThat(location.attributeUnits()).isEmpty();
    }

    @DisplayName("ids are validated at construction rather than on the export thread")
    @Test
    void idsAreValidated() {
        assertThatIllegalArgumentException().isThrownBy(() -> Profile.builder().profileId("0a0b").build());
        assertThatIllegalArgumentException().isThrownBy(() -> new Profile.Link("zz", "0102030405060708"));
        assertThatIllegalArgumentException().isThrownBy(() -> new Profile.Link(ID, "0102"));
    }

    @DisplayName("the original payload is copied on construction and on access")
    @Test
    void originalPayloadIsDefensivelyCopied() {
        var payload = new byte[] {1, 2, 3};
        var profile = profile(payload);

        payload[0] = 99;
        profile.originalPayload()[1] = 99;

        assertThat(profile.originalPayload()).containsExactly(1, 2, 3);
    }

    @DisplayName("equality compares the original payload by value")
    @Test
    void equalityIsByValue() {
        assertThat(profile(new byte[] {1})).isEqualTo(profile(new byte[] {1})).hasSameHashCodeAs(profile(new byte[] {1}));
        assertThat(profile(new byte[] {1})).isNotEqualTo(profile(new byte[] {2}));
    }

    @DisplayName("toBuilder reproduces a fully populated profile")
    @Test
    void toBuilderRoundTrips() {
        var function = new Profile.Function("handle", "_Z6handlev", "handler.cc", 40L);
        var mapping = Profile.Mapping.builder()
                .memoryStart(0x1000L)
                .memoryLimit(0x2000L)
                .fileOffset(0x10L)
                .filename("/bin/app")
                .attributes(Attributes.builder().put("size", 1L).build())
                .attributeUnits(Map.of("size", "By"))
                .build();
        var location = Profile.Location.builder()
                .mapping(mapping)
                .address(0x1234L)
                .addLine(new Profile.Line(function, 44L, 9L))
                .attributes(Attributes.builder().put("profile.frame.type", "native").build())
                .build();
        var sample = Profile.Sample.builder()
                .stack(new Profile.Stack(List.of(location)))
                .attributes(Attributes.builder().put("thread.name", "worker").build())
                .link(new Profile.Link(ID, "0102030405060708"))
                .addValue(7L)
                .addTimestamp(1L)
                .build();
        var profile = Profile.builder()
                .profileId(ID)
                .epochNanos(1L)
                .durationNanos(2L)
                .sampleType(new Profile.ValueType("cpu", "nanoseconds"))
                .periodType(new Profile.ValueType("cpu", "milliseconds"))
                .period(10L)
                .addSample(sample)
                .attributes(Attributes.builder().put("size", 1L).build())
                .attributeUnits(Map.of("size", "By"))
                .droppedAttributesCount(3)
                .originalPayloadFormat("pprof")
                .originalPayload(new byte[] {1})
                .build();

        assertThat(profile.toBuilder().build()).isEqualTo(profile);
        assertThat(sample.toBuilder().build()).isEqualTo(sample);
        assertThat(location.toBuilder().build()).isEqualTo(location);
        assertThat(mapping.toBuilder().build()).isEqualTo(mapping);
    }
}
