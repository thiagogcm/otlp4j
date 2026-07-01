package dev.nthings.otlp4j.config;

import java.io.ByteArrayOutputStream;
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
/// surface. Only general (non-signal-specific) variables are read, only when present. Applied at
/// build time, environment values are lowest precedence: each field is assigned only where the
/// caller did not set it explicitly, so call order does not matter. Present variables are always
/// parsed, so malformed values fail fast even when the field is set explicitly.
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
            var parsed = parseTimeout(timeout);
            if (!builder.timeoutExplicit) {
                builder.setTimeout(parsed);
            }
        }
        var headers = value(env, HEADERS);
        if (headers != null) {
            // Env fills only keys not already set explicitly, so explicit headers win per key.
            parseHeaders(headers).forEach(builder::addHeaderIfAbsent);
        }
        var compression = value(env, COMPRESSION);
        if (compression != null) {
            var parsed = parseCompression(compression);
            if (!builder.compressionExplicit) {
                builder.setCompression(parsed);
            }
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
        var endpoint = ClientConfig.parseEndpoint(raw);
        // Resolve TLS eagerly so malformed cert vars fail fast even when TLS was set explicitly.
        var tls = endpoint.secure() ? resolveTls(env) : Tls.disabled();
        if (!builder.hostExplicit) {
            builder.setHost(endpoint.host());
        }
        if (!builder.portExplicit && endpoint.port() != -1) {
            builder.setPort(endpoint.port());
        }
        if (!builder.pathExplicit) {
            builder.setPath(endpoint.path());
        }
        if (!builder.tlsExplicit) {
            builder.setTls(tls);
        }
    }

    /// TLS when no endpoint dictates the scheme: `INSECURE=true` forces plaintext, else the
    /// certificate variables turn TLS on when present. Otherwise the default is left intact.
    private static void applyTlsWithoutEndpoint(ClientConfig.Builder builder, UnaryOperator<String> env) {
        // Parse/validate eagerly (fail fast) even when TLS was set explicitly; assign only if not.
        var insecure = insecureRequested(env);
        @Nullable Tls resolved = null;
        if (!insecure) {
            var hasCertVars = value(env, CERTIFICATE) != null
                    || value(env, CLIENT_CERTIFICATE) != null
                    || value(env, CLIENT_KEY) != null;
            if (hasCertVars) {
                resolved = resolveTls(env);
            }
        }
        if (builder.tlsExplicit) {
            return;
        }
        if (insecure) {
            builder.setTls(Tls.disabled());
        } else if (resolved != null) {
            builder.setTls(resolved);
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
