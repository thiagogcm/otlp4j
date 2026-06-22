package dev.nthings.otlp4j.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Locale;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// White-box / bug-hunt coverage for [CommonMapper] and the unsigned-int flag handling shared by
/// the trace, metrics and logs mappers. Each test pins the *actual* observed behaviour — the
/// report flags which of these are suspected real production bugs and which are intentional.
@DisplayName("CommonMapper edge cases")
class CommonMapperEdgeCaseTest {

    @DisplayName("CommonMapper.bytes throws on non-hex input")
    @Test
    void bytesThrowsOnNonHexInput() {
        // HEX.parseHex rejects non-hex characters; CommonMapper.bytes does no input validation,
        // so the IllegalArgumentException escapes straight out of the mapper.
        assertThatThrownBy(() -> CommonMapper.bytes("xyz"))
                .as("non-hex id input is not validated and throws from deep in the mapper")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("CommonMapper.bytes throws on odd-length input")
    @Test
    void bytesThrowsOnOddLengthInput() {
        assertThatThrownBy(() -> CommonMapper.bytes("abc"))
                .as("odd-length hex id input is not validated and throws from deep in the mapper")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("Malformed traceId is rejected at construction, before any proto encode")
    @Test
    void malformedTraceIdRejectedAtConstruction() {
        // Validation now lives in the model constructor, so a malformed traceId fails when the Span
        // is built, not later in TraceMapper.toProto on the export thread.
        assertThatThrownBy(() -> TransportFixtures.traceWithTraceId("nothex!!"))
                .as("a malformed traceId is rejected at model construction, not at proto-encode time")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("traceId");
    }

    @DisplayName("Uppercase hex id lowercases through hex(), not a string-identity round-trip")
    @Test
    void uppercaseHexIdIsNotAnIdentityRoundTrip() {
        // bytes() accepts uppercase hex, but hex() always emits lowercase: the byte content is
        // preserved but the string identity is not.
        var upper = "ABCDEF0123456789ABCDEF0123456789";
        var bytes = CommonMapper.bytes(upper);

        assertThat(CommonMapper.hex(bytes))
                .as("hex() always lowercases, so an uppercase id is not a string-identity round-trip")
                .isEqualTo(upper.toLowerCase(Locale.ROOT))
                .isNotEqualTo(upper);
    }

    @DisplayName("Span flags at max unsigned 32-bit value round-trip exactly")
    @Test
    void spanFlagsAtMaxUnsignedIntRoundTrip() {
        // 0xFFFFFFFFL fits in 32 bits: the (int) cast on encode and the & 0xFFFFFFFFL on decode
        // are exact inverses, so this value round-trips.
        var sent = TransportFixtures.traceWithSpanFlags(0xFFFFFFFFL);

        var roundTripped = TraceMapper.toDomain(TraceMapper.toProto(sent)).spans().getFirst();

        assertThat(roundTripped.flags())
                .as("a flags value within the unsigned 32-bit range must round-trip exactly")
                .isEqualTo(0xFFFFFFFFL);
    }

    @DisplayName("Span flags above unsigned 32-bit range are rejected at construction, not silently truncated")
    @Test
    void spanFlagsAboveUnsignedIntRangeAreRejectedAtConstruction() {
        // Bits above bit 31 couldn't survive the (int) encode cast; construction now rejects them
        // instead of silently truncating on the export thread.
        long oversized = 0x1_0000_0001L; // bit 32 set + low bit

        assertThatThrownBy(() -> TransportFixtures.traceWithSpanFlags(oversized))
                .as("flags above the unsigned 32-bit range fail fast at model construction")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flags");
    }
}
