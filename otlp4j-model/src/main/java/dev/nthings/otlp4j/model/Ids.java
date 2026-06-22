package dev.nthings.otlp4j.model;

import java.util.HexFormat;
import java.util.Objects;

/// Construction-time validation for OTLP id and flag fields, so a malformed value fails when the
/// model object is built rather than later on the async export thread (via bad hex or a silent
/// `(int)` narrowing of out-of-range flags).
final class Ids {

    /// A trace id is 16 bytes (32 hex characters).
    static final int TRACE_ID_HEX_LENGTH = 32;

    /// A span id is 8 bytes (16 hex characters).
    static final int SPAN_ID_HEX_LENGTH = 16;

    private static final long UINT32_MAX = 0xFFFF_FFFFL;

    private Ids() {}

    /// Validates a trace id: empty (absent) or exactly 32 hex characters.
    static String traceId(String value) {
        return hexId(value, "traceId", TRACE_ID_HEX_LENGTH);
    }

    /// Validates a span id: empty (absent) or exactly 16 hex characters.
    static String spanId(String value) {
        return hexId(value, "spanId", SPAN_ID_HEX_LENGTH);
    }

    /// Validates a parent span id: empty (absent) or exactly 16 hex characters.
    static String parentSpanId(String value) {
        return hexId(value, "parentSpanId", SPAN_ID_HEX_LENGTH);
    }

    private static String hexId(String value, String field, int hexLength) {
        Objects.requireNonNull(value, field);
        int len = value.length();
        if (len == 0) {
            return value; // an empty id means "absent"
        }
        if (len != hexLength) {
            throw new IllegalArgumentException(
                    field + " must be " + hexLength + " hex characters (or empty), got length " + len);
        }
        for (int i = 0; i < len; i++) {
            char c = value.charAt(i);
            if (!HexFormat.isHexDigit(c)) {
                throw new IllegalArgumentException(
                        field + " must be hex; invalid character '" + c + "' at index " + i);
            }
        }
        return value;
    }

    /// Validates that span/trace flags fit the unsigned 32-bit OTLP wire field; returns them unchanged.
    static long flags(long flags) {
        if (flags < 0 || flags > UINT32_MAX) {
            throw new IllegalArgumentException(
                    "flags must be in the unsigned 32-bit range [0, " + UINT32_MAX + "], got " + flags);
        }
        return flags;
    }
}
