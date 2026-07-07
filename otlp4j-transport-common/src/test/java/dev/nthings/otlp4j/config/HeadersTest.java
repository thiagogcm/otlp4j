package dev.nthings.otlp4j.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@DisplayName("Header validation")
class HeadersTest {

    @DisplayName("valid names pass: alphanumerics, dash, underscore, dot")
    @ParameterizedTest
    @ValueSource(strings = {"authorization", "x-tenant", "X-Custom-1", "a_b.c-d", "grpc-timeout"})
    void validNamesPass(String name) {
        assertThatCode(() -> Headers.requireValidName(name)).doesNotThrowAnyException();
    }

    @DisplayName("invalid names are rejected: empty, spaces, colons, non-ASCII, CR/LF")
    @ParameterizedTest
    @ValueSource(strings = {"", "x tenant", "x:tenant", "häder", "evil\r\nname", "x/y", "x@y"})
    void invalidNamesRejected(String name) {
        assertThatThrownBy(() -> Headers.requireValidName(name))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("the gRPC-reserved -bin suffix is rejected, case-insensitively")
    @ParameterizedTest
    @ValueSource(strings = {"metadata-bin", "trace-BIN", "x-Bin"})
    void binSuffixRejected(String name) {
        assertThatThrownBy(() -> Headers.requireValidName(name))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("-bin");
    }

    @DisplayName("valid values pass: visible ASCII, space, tab")
    @ParameterizedTest
    @ValueSource(strings = {"Bearer abc123", "a\tb", "!#$%&'*+.^_`|~", " leading and trailing "})
    void validValuesPass(String value) {
        assertThatCode(() -> Headers.requireValidValue("k", value)).doesNotThrowAnyException();
    }

    @DisplayName("invalid values are rejected: CR/LF, NUL, other control chars, non-ASCII")
    @ParameterizedTest
    @ValueSource(strings = {"a\r\nb", "a\nb", "a\rb", "a\u0000b", "a\u0007b", "propriétaire", "\u001b[31m"})
    void invalidValuesRejected(String value) {
        assertThatThrownBy(() -> Headers.requireValidValue("secret-key", value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("secret-key");
    }

    @DisplayName("a value failure names the key but never echoes the value")
    @Test
    void failureNeverEchoesValue() {
        assertThatThrownBy(() -> Headers.requireValidValue("authorization", "Bearer sup3r\nsecret"))
                .isInstanceOf(IllegalArgumentException.class)
                .satisfies(e -> assertThat(e.getMessage()).doesNotContain("sup3r"));
    }

    @DisplayName("requireValid validates every entry and returns the argument")
    @Test
    void requireValidValidatesMap() {
        var valid = Map.of("x-tenant", "acme", "authorization", "Bearer x");
        assertThat(Headers.requireValid(valid)).isSameAs(valid);

        assertThatThrownBy(() -> Headers.requireValid(Map.of("x-tenant", "a\r\nb")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Headers.requireValid(Map.of("bad name", "v")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
