package dev.nthings.otlp4j.codec;

import dev.nthings.otlp4j.testing.TransportFixtures;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Locale;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Preserve supported wire values at mapper boundaries.
@DisplayName("CommonMapper edge cases")
class CommonMapperEdgeCaseTest {

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
}
