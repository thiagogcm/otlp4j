package dev.nthings.otlp4j.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Locale;
import org.junit.jupiter.api.Test;

/// White-box / bug-hunt coverage for [CommonMapper] and the unsigned-int flag handling shared by
/// the trace, metrics and logs mappers. Each test pins the *actual* observed behaviour — the
/// report flags which of these are suspected real production bugs and which are intentional.
class CommonMapperEdgeCaseTest {

    @Test
    void bytesThrowsOnNonHexInput() {
        // HEX.parseHex rejects non-hex characters; CommonMapper.bytes does no input validation,
        // so the IllegalArgumentException escapes straight out of the mapper.
        assertThatThrownBy(() -> CommonMapper.bytes("xyz"))
                .as("non-hex id input is not validated and throws from deep in the mapper")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void bytesThrowsOnOddLengthInput() {
        assertThatThrownBy(() -> CommonMapper.bytes("abc"))
                .as("odd-length hex id input is not validated and throws from deep in the mapper")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void malformedTraceIdThrowsOutOfTraceMapperToProto() {
        // A Span with a malformed traceId is accepted by the domain model (no validation there
        // either) and the IllegalArgumentException only surfaces at encode time, from toProto.
        var traces = TransportFixtures.traceWithTraceId("nothex!!");

        assertThatThrownBy(() -> TraceMapper.toProto(traces))
                .as("a malformed traceId is only rejected at proto-encode time, not at construction")
                .isInstanceOf(IllegalArgumentException.class);
    }

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

    @Test
    void spanFlagsAboveUnsignedIntRangeAreTruncated() {
        // A flags value with bits above bit 31 set cannot survive the (int) cast on encode: the
        // high bits are silently dropped. This pins the lossy truncation, not an identity.
        long oversized = 0x1_0000_0001L; // bit 32 set + low bit
        var sent = TransportFixtures.traceWithSpanFlags(oversized);

        var roundTripped = TraceMapper.toDomain(TraceMapper.toProto(sent)).spans().getFirst();

        assertThat(roundTripped.flags())
                .as("flags above the unsigned 32-bit range are truncated by the (int) encode cast")
                .isEqualTo(oversized & 0xFFFFFFFFL)
                .isEqualTo(1L)
                .isNotEqualTo(oversized);
    }
}
