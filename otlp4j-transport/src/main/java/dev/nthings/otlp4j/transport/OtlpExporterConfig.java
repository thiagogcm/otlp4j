package dev.nthings.otlp4j.transport;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.core.policy.TimeoutPolicy;
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

/// The configuration for an OTLP exporter. It is the single source of truth the transport is built
/// from: the endpoint, the OTLP/HTTP path prefix, the TLS mode, the static request headers and an
/// optional per-export header supplier, request-body compression, the neutral [RetryPolicy] (which
/// nests the backoff schedule), and the neutral [TimeoutPolicy] whose per-attempt bound bounds each
/// in-flight RPC or connection and whose total bound bounds the whole retrying delivery end to end. No
/// third-party resilience type appears on it.
///
/// Both the gRPC and HTTP exporter factories accept this one record; each supplies its own default
/// port through [#defaults(int)]. The wire client
/// ([dev.nthings.otlp4j.transport.internal.ArmeriaOtlpClient]) consumes it directly.
///
/// `path` is an OTLP/HTTP endpoint path prefix (e.g. `/otlp`) prepended to the per-signal paths; gRPC
/// ignores it. `headerSupplier`, when set, is evaluated per export and overlays the static `headers`.
///
/// @param host           the endpoint host
/// @param port           the endpoint port
/// @param path           the OTLP/HTTP endpoint path prefix (ignored by gRPC), never null
/// @param tls            the TLS mode
/// @param headers        the static request headers sent on every export
/// @param compression    the request-body compression
/// @param retry          the neutral retry policy
/// @param timeout        the neutral timeout policy
/// @param headerSupplier the per-export header supplier, or null
public record OtlpExporterConfig(
        String host,
        int port,
        String path,
        Tls tls,
        Map<String, String> headers,
        Compression compression,
        RetryPolicy retry,
        TimeoutPolicy timeout,
        @Nullable Supplier<Map<String, String>> headerSupplier)
        implements ComponentConfig {

    /// Validates the components, normalizes the path, and defensively copies and validates the headers.
    public OtlpExporterConfig {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(tls, "tls");
        Objects.requireNonNull(compression, "compression");
        Objects.requireNonNull(retry, "retry");
        Objects.requireNonNull(timeout, "timeout");
        headers = headers == null
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(headers));
        Headers.requireValid(headers);
        path = normalizePath(path);
        Ports.requireValid(port);
    }

    /// The default exporter config for `port`: `localhost:port`, plaintext, no headers, no
    /// compression, the default retry policy, and a per-attempt and total timeout of ten seconds.
    ///
    /// @param port the default endpoint port for the transport (`4317` for gRPC, `4318` for HTTP)
    /// @return the default config
    public static OtlpExporterConfig defaults(int port) {
        return new OtlpExporterConfig(
                "localhost",
                port,
                "",
                Tls.disabled(),
                Map.of(),
                Compression.NONE,
                RetryPolicy.defaults(),
                new TimeoutPolicy(Duration.ofSeconds(10), Duration.ofSeconds(10)),
                null);
    }

    /// A builder seeded with `port` as the endpoint port and all other builder defaults.
    ///
    /// @param port the endpoint port
    /// @return a fresh builder
    public static Builder builder(int port) {
        return new Builder(port);
    }

    /// A builder seeded with this config's fields, so a caller adjusts only what it needs. Every field
    /// counts as explicitly set, so a later [Builder#fromEnvironment()] fills only what this config
    /// left unset.
    ///
    /// @return a builder pre-populated from this config
    public Builder toBuilder() {
        var b = new Builder(port);
        b.host = host;
        b.hostExplicit = true;
        b.portExplicit = true;
        b.path = path;
        b.pathExplicit = true;
        b.tls = tls;
        b.tlsExplicit = true;
        b.headers.putAll(headers);
        b.compression = compression;
        b.compressionExplicit = true;
        b.retry = retry;
        b.perAttempt = timeout.perAttempt();
        b.timeoutExplicit = true;
        b.totalTimeout = timeout.total();
        b.headerSupplier = headerSupplier;
        return b;
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

    /// A parsed `scheme://host:port[/prefix]` endpoint URL. `port` is `-1` when the URL carried none;
    /// `secure` is true for `https`. Package-private seam shared with [OtlpEnv].
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

    /// A fluent builder for [OtlpExporterConfig], covering the endpoint and the common exporter knobs.
    /// The per-attempt and total timeouts are expressed as [Duration] setters here and folded into the
    /// record's [TimeoutPolicy] at [#build()]; an unset total falls back to the per-attempt bound,
    /// reproducing the single-deadline behaviour.
    public static final class Builder {

        private String host = "localhost";
        private int port;
        private String path = "";
        private Tls tls = Tls.disabled();
        private final Map<String, String> headers = new LinkedHashMap<>();
        private Compression compression = Compression.NONE;
        private RetryPolicy retry = RetryPolicy.defaults();
        private Duration perAttempt = Duration.ofSeconds(10);
        private @Nullable Duration totalTimeout = null;
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

        private Builder(int port) {
            this.port = port;
        }

        /// Sets the endpoint from a `scheme://host:port[/prefix]` URL: `https` selects system-trust
        /// TLS, a portless URL keeps the current port. Call [#setTls] afterwards for mTLS.
        ///
        /// @param url the endpoint URL
        /// @return this builder
        public Builder setEndpoint(String url) {
            var e = parseEndpoint(url);
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

        /// Sets the endpoint host and port.
        ///
        /// @param host the endpoint host
        /// @param port the endpoint port
        /// @return this builder
        public Builder setEndpoint(String host, int port) {
            this.host = host;
            this.hostExplicit = true;
            this.port = port;
            this.portExplicit = true;
            return this;
        }

        /// Sets the OTLP/HTTP endpoint path prefix (e.g. `/otlp`); blank or `/` mean none. gRPC
        /// ignores it.
        ///
        /// @param path the path prefix
        /// @return this builder
        public Builder setPath(String path) {
            this.path = path;
            this.pathExplicit = true;
            return this;
        }

        /// Sets the endpoint host.
        ///
        /// @param host the endpoint host
        /// @return this builder
        public Builder setHost(String host) {
            this.host = host;
            this.hostExplicit = true;
            return this;
        }

        /// Sets the endpoint port.
        ///
        /// @param port the endpoint port
        /// @return this builder
        public Builder setPort(int port) {
            this.port = port;
            this.portExplicit = true;
            return this;
        }

        /// Bounds a single export attempt end to end. Unless a larger total is set separately, this is
        /// also the total budget, reproducing the single-deadline behaviour.
        ///
        /// @param timeout the per-attempt deadline
        /// @return this builder
        public Builder setTimeout(Duration timeout) {
            this.perAttempt = timeout;
            this.timeoutExplicit = true;
            return this;
        }

        /// Bounds the whole retrying delivery end to end. Unset, it falls back to [#setTimeout]. Must
        /// be greater than or equal to the per-attempt [#setTimeout].
        ///
        /// @param totalTimeout the total budget across all attempts
        /// @return this builder
        public Builder setTotalTimeout(Duration totalTimeout) {
            this.totalTimeout = totalTimeout;
            return this;
        }

        /// Sets both the per-attempt and total timeout from a [TimeoutPolicy].
        ///
        /// @param timeout the timeout policy
        /// @return this builder
        public Builder setTimeoutPolicy(TimeoutPolicy timeout) {
            this.perAttempt = timeout.perAttempt();
            this.timeoutExplicit = true;
            this.totalTimeout = timeout.total();
            return this;
        }

        /// Selects the TLS mode.
        ///
        /// @param tls the TLS mode
        /// @return this builder
        public Builder setTls(Tls tls) {
            this.tls = tls;
            this.tlsExplicit = true;
            return this;
        }

        /// Adds one request header (e.g. `authorization`), overwriting any existing value for the key.
        ///
        /// @param key   the header name
        /// @param value the header value
        /// @return this builder
        public Builder addHeader(String key, String value) {
            this.headers.put(key, value);
            return this;
        }

        /// Replaces all existing headers with the supplied map.
        ///
        /// @param headers the replacement headers
        /// @return this builder
        public Builder setHeaders(Map<String, String> headers) {
            this.headers.clear();
            this.headers.putAll(headers);
            return this;
        }

        /// Sets a per-export header supplier (e.g. a rotating bearer token). Evaluated on every export
        /// and overlaid on the static headers, winning per key. Does not clear the static
        /// [#addHeader]/[#setHeaders(Map)] entries. Supplied headers are validated on every resolve; an
        /// invalid header fails that export exceptionally and is not retried.
        ///
        /// @param headerSupplier the per-export header supplier
        /// @return this builder
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

        /// Selects request-body compression.
        ///
        /// @param compression the compression mode
        /// @return this builder
        public Builder setCompression(Compression compression) {
            this.compression = compression;
            this.compressionExplicit = true;
            return this;
        }

        /// Familiar string door: `gzip` or `none` (case-insensitive).
        ///
        /// @param compression the compression name
        /// @return this builder
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

        /// Sets the neutral retry policy.
        ///
        /// @param retry the retry policy
        /// @return this builder
        public Builder setRetryPolicy(RetryPolicy retry) {
            this.retry = retry;
            return this;
        }

        /// Applies the standard general `OTEL_EXPORTER_OTLP_*` exporter variables present in the
        /// environment (endpoint URL with any path prefix, timeout, headers, compression, insecure,
        /// TLS cert paths, and protocol). Opt-in and deterministic; read only when present. Environment
        /// values are lowest precedence: applied at [#build()] only where the caller did not set the
        /// field, so call order does not matter. Malformed values throw at build.
        ///
        /// @return this builder
        public Builder fromEnvironment() {
            return fromEnvironment(System::getenv);
        }

        // Package-private seam: tests supply a map-backed lookup so they never read process env. The
        // lookup is buffered and applied at build(), not now, so setters keep precedence regardless of
        // call order.
        Builder fromEnvironment(UnaryOperator<String> env) {
            this.env = env;
            return this;
        }

        /// Builds the config.
        ///
        /// @return the config
        public OtlpExporterConfig build() {
            if (env != null) {
                OtlpEnv.applyTo(this, env);
            }
            var total = totalTimeout == null ? perAttempt : totalTimeout;
            return new OtlpExporterConfig(
                    host, port, path, tls, headers, compression, retry,
                    new TimeoutPolicy(perAttempt, total), headerSupplier);
        }
    }
}
