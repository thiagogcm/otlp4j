package dev.nthings.otlp4j.config;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.jspecify.annotations.Nullable;

/// Applies the standard general OTLP exporter environment variables onto a
/// [ClientConfig.Builder]. Package-private (reached only through
/// [ClientConfig.Builder#fromEnvironment()]), so it stays out of the exported `spi`
/// surface. Only general (non-signal-specific) variables are read, only when present, so explicit
/// setters called after `fromEnvironment()` win; malformed values fail fast.
///
/// TLS follows the endpoint scheme when set; otherwise `OTEL_EXPORTER_OTLP_INSECURE` and the
/// certificate variables apply as independent inputs. An HTTP path prefix is taken from the URL.
final class OtlpEnv {

    static final String ENDPOINT = "OTEL_EXPORTER_OTLP_ENDPOINT";
    static final String HEADERS = "OTEL_EXPORTER_OTLP_HEADERS";
    static final String TIMEOUT = "OTEL_EXPORTER_OTLP_TIMEOUT";
    static final String COMPRESSION = "OTEL_EXPORTER_OTLP_COMPRESSION";
    static final String CERTIFICATE = "OTEL_EXPORTER_OTLP_CERTIFICATE";
    static final String CLIENT_CERTIFICATE = "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE";
    static final String CLIENT_KEY = "OTEL_EXPORTER_OTLP_CLIENT_KEY";
    static final String INSECURE = "OTEL_EXPORTER_OTLP_INSECURE";

    private OtlpEnv() {}

    static void applyTo(ClientConfig.Builder builder, UnaryOperator<String> env) {
        var endpoint = value(env, ENDPOINT);
        if (endpoint != null) {
            // The endpoint scheme decides TLS; INSECURE applies only when no endpoint is set.
            applyEndpoint(builder, endpoint, env);
        } else {
            applyTlsWithoutEndpoint(builder, env);
        }
        var timeout = value(env, TIMEOUT);
        if (timeout != null) {
            builder.timeout(parseTimeout(timeout));
        }
        var headers = value(env, HEADERS);
        if (headers != null) {
            // Merge onto any headers already set: env wins per key but never drops unrelated keys.
            parseHeaders(headers).forEach(builder::header);
        }
        var compression = value(env, COMPRESSION);
        if (compression != null) {
            builder.compression(parseCompression(compression));
        }
    }

    /// The variable's value, or null when unset. Per the OTEL spec an empty value is treated as
    /// unset; a non-empty value is returned verbatim (individual parsers strip and validate it).
    private static @Nullable String value(UnaryOperator<String> env, String key) {
        var raw = env.apply(key);
        return (raw == null || raw.isEmpty()) ? null : raw;
    }

    private static void applyEndpoint(
            ClientConfig.Builder builder, String raw, UnaryOperator<String> env) {
        URI uri;
        try {
            uri = new URI(raw.strip());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(ENDPOINT + " is not a valid URL: " + raw, e);
        }
        var scheme = uri.getScheme();
        if (scheme == null) {
            throw new IllegalArgumentException(
                    ENDPOINT + " must be an absolute http:// or https:// URL: " + raw);
        }
        var tls =
                switch (scheme.toLowerCase(Locale.ROOT)) {
                    case "http" -> false;
                    case "https" -> true;
                    default -> throw new IllegalArgumentException(
                            ENDPOINT + " scheme must be http or https: " + raw);
                };
        var host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException(ENDPOINT + " has no host: " + raw);
        }
        // URI.getHost() wraps an IPv6 literal in brackets; the transport wants the bare address.
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        // No explicit port → keep the builder's protocol default (4317 gRPC, 4318 HTTP). The path
        // prefix is captured for HTTP; gRPC ignores it.
        var port = uri.getPort() == -1 ? builder.port() : uri.getPort();
        builder.host(host).port(port).path(uri.getRawPath());
        // TLS material applies only under https; http is plaintext.
        builder.tls(tls ? resolveTls(env) : Tls.disabled());
    }

    /// TLS when no endpoint dictates the scheme: `INSECURE=true` forces plaintext, else the
    /// certificate variables turn TLS on when present. Otherwise the default is left intact.
    private static void applyTlsWithoutEndpoint(ClientConfig.Builder builder, UnaryOperator<String> env) {
        var hasCertVars = value(env, CERTIFICATE) != null
                || value(env, CLIENT_CERTIFICATE) != null
                || value(env, CLIENT_KEY) != null;
        if (insecureRequested(env)) {
            builder.tls(Tls.disabled());
        } else if (hasCertVars) {
            builder.tls(resolveTls(env));
        }
    }

    /// Whether `OTEL_EXPORTER_OTLP_INSECURE` is `true`; a malformed value fails fast.
    private static boolean insecureRequested(UnaryOperator<String> env) {
        var raw = value(env, INSECURE);
        if (raw == null) {
            return false;
        }
        return switch (raw.strip().toLowerCase(Locale.ROOT)) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException(INSECURE + " must be 'true' or 'false': " + raw);
        };
    }

    private static Tls resolveTls(UnaryOperator<String> env) {
        var clientCert = value(env, CLIENT_CERTIFICATE);
        var clientKey = value(env, CLIENT_KEY);
        var certificate = value(env, CERTIFICATE);
        if (clientCert != null || clientKey != null) {
            if (clientCert == null || clientKey == null) {
                throw new IllegalArgumentException(
                        CLIENT_CERTIFICATE + " and " + CLIENT_KEY + " must be set together");
            }
            return Tls.custom(
                    Path.of(clientCert),
                    Path.of(clientKey),
                    certificate == null ? null : Path.of(certificate));
        }
        if (certificate != null) {
            return Tls.trust(Path.of(certificate));
        }
        return Tls.systemTrust();
    }

    private static Duration parseTimeout(String raw) {
        long millis;
        try {
            millis = Long.parseLong(raw.strip());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    TIMEOUT + " must be an integer number of milliseconds: " + raw, e);
        }
        if (millis <= 0) {
            throw new IllegalArgumentException(TIMEOUT + " must be > 0: " + raw);
        }
        return Duration.ofMillis(millis);
    }

    private static Compression parseCompression(String raw) {
        return switch (raw.strip().toLowerCase(Locale.ROOT)) {
            case "gzip" -> Compression.GZIP;
            case "none" -> Compression.NONE;
            default -> throw new IllegalArgumentException(
                    COMPRESSION + " must be 'gzip' or 'none': " + raw);
        };
    }

    private static Map<String, String> parseHeaders(String raw) {
        var headers = new LinkedHashMap<String, String>();
        for (var pair : raw.split(",")) {
            var entry = pair.strip();
            if (entry.isEmpty()) {
                continue; // tolerate a trailing or doubled comma
            }
            var eq = entry.indexOf('=');
            if (eq < 0) {
                throw new IllegalArgumentException(HEADERS + " entry is missing '=': " + pair);
            }
            var key = entry.substring(0, eq).strip();
            if (key.isEmpty()) {
                throw new IllegalArgumentException(HEADERS + " entry has an empty key: " + pair);
            }
            headers.put(key, percentDecode(entry.substring(eq + 1).strip()));
        }
        return headers;
    }

    /// Decodes W3C-baggage `%XX` octets in a header value. Unlike `URLDecoder`, `+` is left literal.
    private static String percentDecode(String value) {
        if (value.indexOf('%') < 0) {
            return value;
        }
        var bytes = new ByteArrayOutputStream(value.length());
        for (var i = 0; i < value.length(); i++) {
            var c = value.charAt(i);
            if (c != '%') {
                bytes.write(c);
                continue;
            }
            if (i + 2 >= value.length()) {
                throw new IllegalArgumentException(
                        HEADERS + " has a truncated percent-escape: " + value);
            }
            var hi = Character.digit(value.charAt(i + 1), 16);
            var lo = Character.digit(value.charAt(i + 2), 16);
            if (hi < 0 || lo < 0) {
                throw new IllegalArgumentException(
                        HEADERS + " has an invalid percent-escape: " + value);
            }
            bytes.write((hi << 4) + lo);
            i += 2;
        }
        return bytes.toString(StandardCharsets.UTF_8);
    }
}
