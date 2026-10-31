package dev.nthings.otlp4j.transport;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.RedeliveryPolicy;
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
/// ## Queue and redelivery
///
/// `queue` and `redelivery` govern the exporter as a graph component, where the exporter id gets one
/// [DeliveryQueuePolicy] queue per signal, as the upstream Collector exporter does. A payload is
/// acknowledged to its sender once it is in the queue; the queue's workers then export it, and when the
/// destination fails retryably they redeliver it under `redelivery`, so the data survives an outage
/// that outlasts the short wire-level `retry`. A full queue refuses the arriving payload retryably, and
/// the refusal reaches the sender as `503` or `UNAVAILABLE`. `queue` is `null` to disable queueing, for
/// a graph exporter that must relay a downstream failure to its sender synchronously. The default is
/// [DeliveryQueuePolicy#defaults()] and [RedeliveryPolicy#defaults()].
///
/// A standalone [OtlpExporter], built with `grpcBuilder()` or `httpBuilder()` and driven by `export`, is
/// never queued and ignores `queue` and `redelivery`: its caller gets the real end-to-end result of the
/// wire attempt, bounded by `retry` and `timeout`, and decides for itself what to do with a failure.
/// The queue's redelivery wraps that bounded attempt, so one redelivery attempt is one `export`.
///
/// @param host           the endpoint host
/// @param port           the endpoint port
/// @param path           the OTLP/HTTP endpoint path prefix (ignored by gRPC), never null
/// @param tls            the TLS mode
/// @param headers        the static request headers sent on every export
/// @param compression    the request-body compression
/// @param retry          the neutral retry policy of one wire attempt
/// @param timeout        the neutral timeout policy
/// @param headerSupplier the per-export header supplier, or null
/// @param maxRequestBytes  the cap on one uncompressed request
/// @param maxResponseBytes the cap on one decompressed response
/// @param queue          the per-signal queue of a graph exporter, or null for none
/// @param redelivery     how the queue redelivers a retryable failure
public record OtlpExporterConfig(
        String host,
        int port,
        String path,
        Tls tls,
        Map<String, String> headers,
        Compression compression,
        RetryPolicy retry,
        TimeoutPolicy timeout,
        @Nullable Supplier<Map<String, String>> headerSupplier,
        long maxRequestBytes,
        long maxResponseBytes,
        @Nullable DeliveryQueuePolicy queue,
        RedeliveryPolicy redelivery)
        implements ComponentConfig {

    /// Validates the components, normalizes the path, and defensively copies and validates the headers.
    public OtlpExporterConfig {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(tls, "tls");
        Objects.requireNonNull(compression, "compression");
        Objects.requireNonNull(retry, "retry");
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(redelivery, "redelivery");
        headers = headers == null
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(headers));
        Headers.requireValid(headers);
        path = normalizePath(path);
        Ports.requireValid(port);
        if (maxRequestBytes <= 0) {
            throw new IllegalArgumentException("maxRequestBytes must be positive: " + maxRequestBytes);
        }
        if (maxResponseBytes <= 0) {
            throw new IllegalArgumentException("maxResponseBytes must be positive: " + maxResponseBytes);
        }
        // The gRPC client builder accepts int byte limits. Keep this shared configuration
        // representable for both protocol implementations so invalid values fail at build time,
        // rather than only when a gRPC exporter is started.
        if (maxRequestBytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "maxRequestBytes must be <= " + Integer.MAX_VALUE + ": " + maxRequestBytes);
        }
        if (maxResponseBytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "maxResponseBytes must be <= " + Integer.MAX_VALUE + ": " + maxResponseBytes);
        }
    }

    /// The default exporter config for `port`: `localhost:port`, plaintext, no headers, no
    /// compression, the default retry policy, a per-attempt and total timeout of ten seconds, and the
    /// default queue and redelivery.
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
                null,
                64L * 1024 * 1024,
                4L * 1024 * 1024,
                DeliveryQueuePolicy.defaults(),
                RedeliveryPolicy.defaults());
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
        b.maxRequestBytes = maxRequestBytes;
        b.maxResponseBytes = maxResponseBytes;
        b.queue = queue;
        b.redelivery = redelivery;
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

    /// A parsed `scheme://host[:port][/prefix]` endpoint URL. `port` is always concrete: the URL's own,
    /// else the scheme default (`80` for `http`, `443` for `https`). `secure` is true for `https`.
    /// Package-private seam shared with [OtlpEnv].
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
        // The OTLP exporter spec forbids altering the URL otherwise, so a portless URL means the
        // scheme's well-known port, never the transport's 4317/4318 seed.
        var parsedPort = uri.getPort() != -1 ? uri.getPort() : (secure ? 443 : 80);
        var rawPath = uri.getRawPath();
        return new Endpoint(parsedHost, parsedPort, rawPath == null ? "" : rawPath, secure);
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
        private long maxRequestBytes = 64L * 1024 * 1024;
        private long maxResponseBytes = 4L * 1024 * 1024;
        private @Nullable DeliveryQueuePolicy queue = DeliveryQueuePolicy.defaults();
        private RedeliveryPolicy redelivery = RedeliveryPolicy.defaults();

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

        /// Sets the endpoint from a `scheme://host[:port][/prefix]` URL: `https` selects system-trust
        /// TLS. The URL decides the port: an explicit one, else `80` for `http` and `443` for `https`
        /// (not the transport's 4317/4318 default). Call [#tls] afterwards for mTLS.
        ///
        /// @param url the endpoint URL
        /// @return this builder
        public Builder endpoint(String url) {
            var e = parseEndpoint(url);
            this.host = e.host();
            this.hostExplicit = true;
            this.path = e.path();
            this.pathExplicit = true;
            this.port = e.port();
            this.portExplicit = true;
            this.tls = e.secure() ? Tls.systemTrust() : Tls.disabled();
            this.tlsExplicit = true;
            return this;
        }

        /// Sets the endpoint host and port.
        ///
        /// @param host the endpoint host
        /// @param port the endpoint port
        /// @return this builder
        public Builder endpoint(String host, int port) {
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
        public Builder path(String path) {
            this.path = path;
            this.pathExplicit = true;
            return this;
        }

        /// Sets the endpoint host.
        ///
        /// @param host the endpoint host
        /// @return this builder
        public Builder host(String host) {
            this.host = host;
            this.hostExplicit = true;
            return this;
        }

        /// Sets the endpoint port.
        ///
        /// @param port the endpoint port
        /// @return this builder
        public Builder port(int port) {
            this.port = port;
            this.portExplicit = true;
            return this;
        }

        /// Bounds a single export attempt end to end. Unless a larger total is set separately, this is
        /// also the total budget, reproducing the single-deadline behaviour.
        ///
        /// @param timeout the per-attempt deadline
        /// @return this builder
        public Builder timeout(Duration timeout) {
            this.perAttempt = timeout;
            this.timeoutExplicit = true;
            return this;
        }

        /// Bounds the whole retrying delivery end to end. Unset, it falls back to [#timeout]. Must
        /// be greater than or equal to the per-attempt [#timeout].
        ///
        /// @param totalTimeout the total budget across all attempts
        /// @return this builder
        public Builder totalTimeout(Duration totalTimeout) {
            this.totalTimeout = totalTimeout;
            return this;
        }

        /// Sets both the per-attempt and total timeout from a [TimeoutPolicy].
        ///
        /// @param timeout the timeout policy
        /// @return this builder
        public Builder timeoutPolicy(TimeoutPolicy timeout) {
            this.perAttempt = timeout.perAttempt();
            this.timeoutExplicit = true;
            this.totalTimeout = timeout.total();
            return this;
        }

        /// Selects the TLS mode.
        ///
        /// @param tls the TLS mode
        /// @return this builder
        public Builder tls(Tls tls) {
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
        public Builder headers(Map<String, String> headers) {
            this.headers.clear();
            this.headers.putAll(headers);
            return this;
        }

        /// Sets a per-export header supplier (e.g. a rotating bearer token). Evaluated on every export
        /// and overlaid on the static headers, winning per key. Does not clear the static
        /// [#addHeader]/[#headers(Map)] entries. Supplied headers are validated on every resolve; an
        /// invalid header fails that export exceptionally and is not retried.
        ///
        /// @param headerSupplier the per-export header supplier
        /// @return this builder
        public Builder headers(Supplier<Map<String, String>> headerSupplier) {
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
        public Builder compression(Compression compression) {
            this.compression = compression;
            this.compressionExplicit = true;
            return this;
        }

        /// Familiar string door: `gzip` or `none` (case-insensitive).
        ///
        /// @param compression the compression name
        /// @return this builder
        public Builder compression(String compression) {
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
        public Builder retryPolicy(RetryPolicy retry) {
            this.retry = retry;
            return this;
        }

        /// Sets the per-signal queue a graph exporter acknowledges into. Replaces the default
        /// ([DeliveryQueuePolicy#defaults()]); has no effect on a standalone [OtlpExporter].
        ///
        /// @param queue the queue policy
        /// @return this builder
        public Builder queue(DeliveryQueuePolicy queue) {
            this.queue = Objects.requireNonNull(queue, "queue");
            return this;
        }

        /// Disables the queue, so a graph exporter exports synchronously and relays each failure to its
        /// sender. Nothing redelivers, so `redelivery` is unused.
        ///
        /// @return this builder
        public Builder noQueue() {
            this.queue = null;
            return this;
        }

        /// Sets how the queue redelivers a retryable failure. Replaces the default
        /// ([RedeliveryPolicy#defaults()]); unused without a queue.
        ///
        /// @param redelivery the redelivery policy
        /// @return this builder
        public Builder redelivery(RedeliveryPolicy redelivery) {
            this.redelivery = Objects.requireNonNull(redelivery, "redelivery");
            return this;
        }

        /// Caps one uncompressed export request. The cap is checked before and after request
        /// compression, and is therefore also a bound on the bytes passed to a remote peer.
        public Builder maxRequestBytes(long bytes) {
            this.maxRequestBytes = bytes;
            return this;
        }

        /// Caps one response after decompression. Armeria also applies the cap while aggregating
        /// compressed responses, so a decompressor cannot bypass it.
        public Builder maxResponseBytes(long bytes) {
            this.maxResponseBytes = bytes;
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
                    new TimeoutPolicy(perAttempt, total), headerSupplier,
                    maxRequestBytes, maxResponseBytes, queue, redelivery);
        }
    }
}
