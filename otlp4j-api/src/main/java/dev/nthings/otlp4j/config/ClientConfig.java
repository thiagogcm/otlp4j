package dev.nthings.otlp4j.config;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
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

        public Builder endpoint(String host, int port) {
            this.host = host;
            this.port = port;
            return this;
        }

        /// Sets the OTLP/HTTP endpoint path prefix (e.g. `/otlp`); blank or `/` mean none. gRPC
        /// ignores it.
        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /// The port currently set on this builder. Package-private seam used by [OtlpEnv] so an
        /// endpoint URL without an explicit port falls back to the protocol's default (4317 for
        /// gRPC, 4318 for HTTP) rather than a single hardcoded constant.
        int port() {
            return port;
        }

        public Builder timeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder tls(Tls tls) {
            this.tls = tls;
            return this;
        }

        public Builder header(String key, String value) {
            this.headers.put(key, value);
            return this;
        }

        /// Replaces all existing headers with the supplied map.
        public Builder headers(Map<String, String> headers) {
            this.headers.clear();
            this.headers.putAll(headers);
            return this;
        }

        /// Adds all of `headers` on top of any already set; values overwrite existing ones per key.
        public Builder addHeaders(Map<String, String> headers) {
            this.headers.putAll(headers);
            return this;
        }

        public Builder compression(Compression compression) {
            this.compression = compression;
            return this;
        }

        public Builder retry(RetryPolicy retry) {
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
