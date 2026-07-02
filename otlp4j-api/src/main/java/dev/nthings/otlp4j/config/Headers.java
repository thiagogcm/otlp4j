package dev.nthings.otlp4j.config;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/// Validation for OTLP export request headers, applied when a [ClientConfig] is built (static
/// headers) and on every resolve of a per-export header supplier.
///
/// The rules are the intersection both bundled transports accept, so an invalid header fails
/// fast at configuration time instead of on the export path:
///
///   - names: non-empty, characters from `[0-9a-zA-Z_.-]` (gRPC metadata's exact set; HTTP token
///     names are a superset), and not the gRPC-reserved `-bin` suffix.
///   - values: visible ASCII plus horizontal tab and space - no CR/LF (header injection), no
///     NUL, no non-ASCII (the gRPC ASCII marshaller would fail at send time).
///
/// Validation failures name the offending header key but never echo its value, which may be a
/// credential.
public final class Headers {

    private Headers() {}

    /// Validates one header name.
    ///
    /// @param name the header name
    /// @throws IllegalArgumentException when the name is empty, contains a character outside
    ///                                  `[0-9a-zA-Z_.-]`, or ends with the reserved `-bin` suffix
    public static void requireValidName(String name) {
        Objects.requireNonNull(name, "header name");
        if (name.isEmpty()) {
            throw new IllegalArgumentException("header name must not be empty");
        }
        for (var i = 0; i < name.length(); i++) {
            var c = name.charAt(i);
            var valid = (c >= '0' && c <= '9')
                    || (c >= 'a' && c <= 'z')
                    || (c >= 'A' && c <= 'Z')
                    || c == '_' || c == '.' || c == '-';
            if (!valid) {
                throw new IllegalArgumentException(
                        "header name '" + name + "' contains an invalid character at index " + i);
            }
        }
        if (name.toLowerCase(Locale.ROOT).endsWith("-bin")) {
            throw new IllegalArgumentException(
                    "header name '" + name + "' uses the gRPC-reserved -bin suffix");
        }
    }

    /// Validates one header value. The failure message names only the key, never the value.
    ///
    /// @param name  the header name, used in the failure message
    /// @param value the header value
    /// @throws IllegalArgumentException when the value contains CR/LF, NUL, other control
    ///                                  characters, or non-ASCII characters
    public static void requireValidValue(String name, String value) {
        Objects.requireNonNull(value, () -> "header '" + name + "' has a null value");
        for (var i = 0; i < value.length(); i++) {
            var c = value.charAt(i);
            var valid = c == '\t' || (c >= 0x20 && c <= 0x7E);
            if (!valid) {
                throw new IllegalArgumentException(
                        "header '" + name + "' value contains an invalid character at index " + i);
            }
        }
    }

    /// Validates every entry of `headers` and returns the argument for fluent use.
    ///
    /// @param headers the headers to validate
    /// @return the validated `headers` argument
    /// @throws IllegalArgumentException when any name or value is invalid
    public static Map<String, String> requireValid(Map<String, String> headers) {
        Objects.requireNonNull(headers, "headers");
        for (var entry : headers.entrySet()) {
            requireValidName(entry.getKey());
            requireValidValue(entry.getKey(), entry.getValue());
        }
        return headers;
    }
}
