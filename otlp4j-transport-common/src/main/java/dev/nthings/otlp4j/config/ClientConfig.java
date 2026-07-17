package dev.nthings.otlp4j.config;

import dev.nthings.otlp4j.core.policy.RetryPolicy;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.jspecify.annotations.Nullable;

/// Configuration for an OTLP exporter transport.
///
/// Defaults via [#builder]: `localhost:4317`, 10s deadline, TLS disabled, no headers,
/// no compression, the default [RetryPolicy] on, no path prefix. `path` is an OTLP/HTTP endpoint path
/// prefix (e.g. `/otlp`) prepended to the per-signal paths; gRPC ignores it.
/// `headerSupplier`, when set, is evaluated per export and overlays the static `headers`.
///
/// `timeout` is the per-attempt deadline bounding each in-flight RPC or connection, while
/// `totalTimeout` is the total budget bounding the whole retrying delivery end to end. Unset,
/// `totalTimeout` falls back to `timeout`, which reproduces the single-deadline behaviour. It must be
/// greater than or equal to `timeout`.
public record ClientConfig(
        String host,
        int port,
        String path,
        Duration timeout,
        Tls tls,
        Map<String, String> headers,
        Compression compression,
        RetryPolicy retry,
        @Nullable Supplier<Map<String, String>> headerSupplier,
        Duration totalTimeout) {

    private static final RetryPolicy DEFAULT_RETRY = RetryPolicy.defaults();

    public ClientConfig {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(tls, "tls");
        Objects.requireNonNull(compression, "compression");
        Objects.requireNonNull(retry, "retry");
        Objects.requireNonNull(totalTimeout, "totalTimeout");
        headers = headers == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(headers));
        Headers.requireValid(headers);
        path = normalizePath(path);
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be > 0");
        }
        if (totalTimeout.compareTo(timeout) < 0) {
            throw new IllegalArgumentException("totalTimeout must be >= timeout");
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

    /// The default retry policy: 5 attempts, 1s initial exponential backoff, 1.5x multiplier, 5s
    /// maximum backoff, and a 0.5 jitter factor, honouring a server-sent retry-after hint.
    public static RetryPolicy defaultRetryPolicy() {
        return DEFAULT_RETRY;
    }

    /// The default config (all builder defaults, `localhost:4317` plaintext).
    public static ClientConfig defaults() {
        return builder().build();
    }

    /// Returns a builder pre-populated with this config's fields. Every field counts as explicitly
    /// set, so a later [Builder#fromEnvironment()] fills only what this config left unset.
    public Builder toBuilder() {
        var b = new Builder();
        b.host = host;
        b.hostExplicit = true;
        b.port = port;
        b.portExplicit = true;
        b.timeout = timeout;
        b.timeoutExplicit = true;
        b.totalTimeout = totalTimeout;
        b.tls = tls;
        b.tlsExplicit = true;
        b.headers.putAll(headers);
        b.compression = compression;
        b.compressionExplicit = true;
        b.retry = retry;
        b.path = path;
        b.pathExplicit = true;
        b.headerSupplier = headerSupplier;
        return b;
    }

    /// A parsed `scheme://host:port[/prefix]` endpoint URL. `port` is `-1` when the URL carried
    /// none; `secure` is true for `https`. Package-private seam shared with [OtlpEnv].
    record Endpoint(String host, int port, String path, boolean secure) {}

    /// Parses an http/https endpoint URL into its parts without mutating a builder, so callers can
    /// apply each part independently. Package-private seam shared with [OtlpEnv].
    static Endpoint parseEndpoint(String url) {
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
        var rawPath = uri.getRawPath();
        return new Endpoint(parsedHost, uri.getPort(), rawPath == null ? "" : rawPath, secure);
    }

    public static final class Builder {

        private String host = "localhost";
        private int port = 4317;
        private Duration timeout = Duration.ofSeconds(10);
        private @Nullable Duration totalTimeout = null;
        private Tls tls = Tls.disabled();
        private final Map<String, String> headers = new LinkedHashMap<>();
        private Compression compression = Compression.NONE;
        private RetryPolicy retry = DEFAULT_RETRY;
        private String path = "";
        private @Nullable Supplier<Map<String, String>> headerSupplier = null;

        // Which fields the caller set explicitly, so a deferred fromEnvironment() overrides only the
        // untouched ones (env is lowest precedence). Package-private: read/written by [OtlpEnv].
        boolean hostExplicit;
        boolean portExplicit;
        boolean pathExplicit;
        boolean tlsExplicit;
        boolean timeoutExplicit;
        boolean compressionExplicit;
        private @Nullable UnaryOperator<String> env = null;

        private Builder() {}

        /// Sets the endpoint from a `scheme://host:port[/prefix]` URL: `https` selects system-trust
        /// TLS, a portless URL keeps the protocol default. Call [#setTls] afterwards for mTLS.
        public Builder setEndpoint(String url) {
            var e = ClientConfig.parseEndpoint(url);
            this.host = e.host();
            this.hostExplicit = true;
            this.path = e.path();
            this.pathExplicit = true;
            if (e.port() != -1) {
                this.port = e.port();
                this.portExplicit = true;
            }
            this.tls = e.secure() ? Tls.systemTrust() : Tls.disabled();
            this.tlsExplicit = true;
            return this;
        }

        public Builder setEndpoint(String host, int port) {
            this.host = host;
            this.hostExplicit = true;
            this.port = port;
            this.portExplicit = true;
            return this;
        }

        /// Sets the OTLP/HTTP endpoint path prefix (e.g. `/otlp`); blank or `/` mean none. gRPC
        /// ignores it.
        public Builder setPath(String path) {
            this.path = path;
            this.pathExplicit = true;
            return this;
        }

        public Builder setHost(String host) {
            this.host = host;
            this.hostExplicit = true;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            this.portExplicit = true;
            return this;
        }

        public Builder setTimeout(Duration timeout) {
            this.timeout = timeout;
            this.timeoutExplicit = true;
            return this;
        }

        /// Bounds the whole retrying delivery end to end. Unset, it falls back to [#setTimeout],
        /// reproducing the single-deadline behaviour. Must be greater than or equal to the per-attempt
        /// [#setTimeout].
        public Builder setTotalTimeout(Duration totalTimeout) {
            this.totalTimeout = totalTimeout;
            return this;
        }

        public Builder setTls(Tls tls) {
            this.tls = tls;
            this.tlsExplicit = true;
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

        /// Sets a per-export header supplier (e.g. a rotating bearer token). Evaluated on every
        /// export and overlaid on the static headers, winning per key. Does not clear the static
        /// [#addHeader]/[#setHeaders(Map)] entries. Supplied headers are validated (see
        /// [Headers]) on every resolve; an invalid header fails that export exceptionally and is
        /// not retried.
        public Builder setHeaders(Supplier<Map<String, String>> headerSupplier) {
            this.headerSupplier = headerSupplier;
            return this;
        }

        // Adds an env-sourced header only when the key is not already set explicitly, so explicit
        // headers win. Package-private seam for [OtlpEnv].
        Builder addHeaderIfAbsent(String key, String value) {
            this.headers.putIfAbsent(key, value);
            return this;
        }

        public Builder setCompression(Compression compression) {
            this.compression = compression;
            this.compressionExplicit = true;
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
            this.compressionExplicit = true;
            return this;
        }

        public Builder setRetryPolicy(RetryPolicy retry) {
            this.retry = retry;
            return this;
        }

        /// Applies the standard general `OTEL_EXPORTER_OTLP_*` exporter variables present in the
        /// environment (endpoint URL with any path prefix, timeout, headers, compression, insecure,
        /// TLS cert paths, and protocol). Opt-in and deterministic; read only when present.
        /// Environment values are lowest precedence: applied at [#build()] only where the caller did
        /// not set the field, so call order does not matter. Malformed values throw
        /// [IllegalArgumentException] at build.
        ///
        /// **Implemented subset.** `OTEL_EXPORTER_OTLP_ENDPOINT`, `_HEADERS`, `_TIMEOUT`,
        /// `_COMPRESSION`, `_CERTIFICATE`, `_CLIENT_CERTIFICATE`, `_CLIENT_KEY`, and `_INSECURE` fill
        /// this config's fields; `_PROTOCOL` is validated (`grpc` and `http/protobuf` accepted,
        /// `http/json` rejected as this gateway is protobuf-only) but does not select the transport,
        /// which remains the entry point the caller chose. The per-signal
        /// `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_*` overrides and any server-side (receiver)
        /// environment variables are out of scope: this gateway is one endpoint, one channel, four
        /// facets, so there is no per-signal destination to route them to.
        public Builder fromEnvironment() {
            return fromEnvironment(System::getenv);
        }

        // Package-private seam: tests supply a map-backed lookup so they never read process env. The
        // lookup is buffered and applied at build(), not now, so setters keep precedence regardless
        // of call order.
        Builder fromEnvironment(UnaryOperator<String> env) {
            this.env = env;
            return this;
        }

        public ClientConfig build() {
            if (env != null) {
                OtlpEnv.applyTo(this, env);
            }
            return new ClientConfig(
                    host, port, path, timeout, tls, headers, compression, retry, headerSupplier,
                    totalTimeout == null ? timeout : totalTimeout);
        }
    }
}
