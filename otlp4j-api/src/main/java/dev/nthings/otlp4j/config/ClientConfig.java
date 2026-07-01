package dev.nthings.otlp4j.config;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

/// Configuration for an OTLP exporter transport.
///
/// Defaults via [#builder]: `localhost:4317`, 10s deadline, TLS disabled, no headers,
/// no compression, no retries, no path prefix. `path` is an OTLP/HTTP endpoint path prefix
/// (e.g. `/otlp`) prepended to the per-signal paths; gRPC ignores it.
public record ClientConfig(
        String host,
        int port,
        String path,
        Duration timeout,
        Tls tls,
        Map<String, String> headers,
        Compression compression,
        RetryPolicy retry) {

    public ClientConfig {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(tls, "tls");
        Objects.requireNonNull(compression, "compression");
        Objects.requireNonNull(retry, "retry");
        headers = headers == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(headers));
        path = normalizePath(path);
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be > 0");
        }
    }

    /// Normalizes a path prefix to `""` or a `/`-led form with no trailing slash; blank and `/`
    /// collapse to `""`.
    private static String normalizePath(String path) {
        if (path == null) {
            return "";
        }
        var p = path.strip();
        while (p.endsWith("/")) {
            p = p.substring(0, p.length() - 1);
        }
        if (p.isEmpty()) {
            return "";
        }
        return p.startsWith("/") ? p : "/" + p;
    }

    public static Builder builder() {
        return new Builder();
    }

    /// The default config (all builder defaults, `localhost:4317` plaintext).
    public static ClientConfig getDefault() {
        return builder().build();
    }

    /// Returns a builder pre-populated with this config's fields.
    public Builder toBuilder() {
        var b = new Builder();
        b.host = host;
        b.port = port;
        b.timeout = timeout;
        b.tls = tls;
        b.headers.putAll(headers);
        b.compression = compression;
        b.retry = retry;
        b.path = path;
        return b;
    }

    public static final class Builder {

        private String host = "localhost";
        private int port = 4317;
        private Duration timeout = Duration.ofSeconds(10);
        private Tls tls = Tls.disabled();
        private final Map<String, String> headers = new LinkedHashMap<>();
        private Compression compression = Compression.NONE;
        private RetryPolicy retry = RetryPolicy.none();
        private String path = "";

        private Builder() {}

        /// Sets the endpoint from a `scheme://host:port[/prefix]` URL: `https` selects system-trust
        /// TLS, a portless URL keeps the protocol default. Call [#setTls] afterwards for mTLS.
        public Builder setEndpoint(String url) {
            this.tls = applyEndpointUrl(url) ? Tls.systemTrust() : Tls.disabled();
            return this;
        }

        public Builder setEndpoint(String host, int port) {
            this.host = host;
            this.port = port;
            return this;
        }

        /// Parses an http/https endpoint URL onto host/port/path, returning whether the scheme
        /// selects TLS. Package-private seam shared with [OtlpEnv].
        boolean applyEndpointUrl(String url) {
            URI uri;
            try {
                uri = new URI(url.strip());
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("endpoint is not a valid URL: " + url, e);
            }
            var scheme = uri.getScheme();
            if (scheme == null) {
                throw new IllegalArgumentException(
                        "endpoint must be an absolute http:// or https:// URL: " + url);
            }
            var secure =
                    switch (scheme.toLowerCase(Locale.ROOT)) {
                        case "http" -> false;
                        case "https" -> true;
                        default -> throw new IllegalArgumentException(
                                "endpoint scheme must be http or https: " + url);
                    };
            var parsedHost = uri.getHost();
            if (parsedHost == null || parsedHost.isBlank()) {
                throw new IllegalArgumentException("endpoint has no host: " + url);
            }
            // URI.getHost() wraps an IPv6 literal in brackets; the transport wants the bare address.
            if (parsedHost.startsWith("[") && parsedHost.endsWith("]")) {
                parsedHost = parsedHost.substring(1, parsedHost.length() - 1);
            }
            this.host = parsedHost;
            // No explicit port keeps the current protocol default (4317 gRPC, 4318 HTTP).
            if (uri.getPort() != -1) {
                this.port = uri.getPort();
            }
            this.path = uri.getRawPath();
            return secure;
        }

        /// Sets the OTLP/HTTP endpoint path prefix (e.g. `/otlp`); blank or `/` mean none. gRPC
        /// ignores it.
        public Builder setPath(String path) {
            this.path = path;
            return this;
        }

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setTimeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder setTls(Tls tls) {
            this.tls = tls;
            return this;
        }

        /// Adds one request header (e.g. `authorization`), overwriting any existing value for the key.
        public Builder addHeader(String key, String value) {
            this.headers.put(key, value);
            return this;
        }

        /// Replaces all existing headers with the supplied map.
        public Builder setHeaders(Map<String, String> headers) {
            this.headers.clear();
            this.headers.putAll(headers);
            return this;
        }

        public Builder setCompression(Compression compression) {
            this.compression = compression;
            return this;
        }

        /// Familiar string door: `gzip` or `none` (case-insensitive).
        public Builder setCompression(String compression) {
            this.compression =
                    switch (compression.strip().toLowerCase(Locale.ROOT)) {
                        case "gzip" -> Compression.GZIP;
                        case "none" -> Compression.NONE;
                        default -> throw new IllegalArgumentException(
                                "compression must be 'gzip' or 'none': " + compression);
                    };
            return this;
        }

        public Builder setRetryPolicy(RetryPolicy retry) {
            this.retry = retry;
            return this;
        }

        /// Applies the standard general `OTEL_EXPORTER_OTLP_*` exporter variables present in the
        /// environment (endpoint URL with any path prefix, timeout, headers, compression, insecure,
        /// TLS cert paths). Opt-in and deterministic; read only on this call. Call it first so
        /// explicit setters afterwards win. Malformed values throw [IllegalArgumentException].
        public Builder fromEnvironment() {
            return fromEnvironment(System::getenv);
        }

        // Package-private seam: tests supply a map-backed lookup so they never read process env.
        Builder fromEnvironment(UnaryOperator<String> env) {
            OtlpEnv.applyTo(this, env);
            return this;
        }

        public ClientConfig build() {
            return new ClientConfig(host, port, path, timeout, tls, headers, compression, retry);
        }
    }
}
