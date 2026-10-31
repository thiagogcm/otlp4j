package dev.nthings.otlp4j.transport.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/// The server table of the OTLP Profiles development version. The wire suites cover how each protocol
/// reports a refusal.
@DisplayName("Profiles development version")
class ProfilesDevelopmentVersionTest {

    @DisplayName("a request without the metadata is version 1")
    @Test
    void absentMeansVersionOne() {
        assertThat(ProfilesDevelopmentVersion.rejection(List.of())).isNull();
    }

    @DisplayName("version 1 is accepted")
    @Test
    void acceptsTheSupportedVersion() {
        assertThat(ProfilesDevelopmentVersion.rejection(List.of("1"))).isNull();
    }

    @DisplayName("anything else is refused: another version, a malformed value, or a repeated one")
    @ParameterizedTest
    @MethodSource("refusedValues")
    void refusesEverythingElse(List<String> values) {
        assertThat(ProfilesDevelopmentVersion.rejection(values)).isNotNull();
    }

    static Stream<List<String>> refusedValues() {
        return Stream.of(
                List.of("2"),
                List.of("99999999999999999999999"),
                List.of(""),
                List.of("0"),
                List.of("01"),
                List.of("-1"),
                List.of("+1"),
                List.of(" 1"),
                List.of("1,1"),
                List.of("1.0"),
                List.of("one"),
                List.of("1", "1"));
    }
}
