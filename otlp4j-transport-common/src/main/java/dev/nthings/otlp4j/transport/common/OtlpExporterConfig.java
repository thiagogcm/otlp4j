package dev.nthings.otlp4j.transport.common;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.core.policy.TimeoutPolicy;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/// The neutral, transport-independent configuration for an OTLP exporter component. It carries the
/// endpoint, TLS mode, request headers, compression, the neutral [RetryPolicy] (which itself nests
/// the backoff schedule), and the neutral [TimeoutPolicy], and no third-party resilience type appears
/// on it. Both the gRPC and HTTP exporter factories accept this one record; each supplies its own
/// default port through [#defaults(int)]. The factory translates it to the internal
/// [ClientConfig] through [#toClientConfig()], where the neutral policies are handed to the legacy
/// transport client, which is the single place the resilience library is still named.
///
/// The [TimeoutPolicy]'s per-attempt bound maps onto the legacy client's single per-export deadline;
/// its total bound is carried for the exporter runtime a later phase brings and is not yet consumed
/// by the legacy client, whose own retry loop bounds each attempt independently.
///
/// @param host           the endpoint host
/// @param port           the endpoint port
/// @param path           the OTLP/HTTP endpoint path prefix (ignored by gRPC), never null
/// @param tls            the TLS mode
/// @param headers        the static request headers sent on every export
/// @param compression    the request-body compression
/// @param retry          the neutral retry policy
/// @param timeout        the neutral timeout policy
/// @param connectTimeout the optional OTLP/HTTP connection-setup bound, may be null
public record OtlpExporterConfig(
        String host,
        int port,
        String path,
        Tls tls,
        Map<String, String> headers,
        Compression compression,
        RetryPolicy retry,
        TimeoutPolicy timeout,
        @Nullable Duration connectTimeout)
        implements ComponentConfig {

    /// Validates the components and defensively copies the headers.
    public OtlpExporterConfig {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(tls, "tls");
        Objects.requireNonNull(compression, "compression");
        Objects.requireNonNull(retry, "retry");
        Objects.requireNonNull(timeout, "timeout");
        path = path == null ? "" : path;
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
    }

    /// The default exporter config for `port`: `localhost:port`, plaintext, no headers, no
    /// compression, the default retry policy, and a per-attempt and total timeout of ten seconds
    /// (matching the legacy client's default per-export deadline).
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

    /// A builder seeded with this config's fields, so a caller adjusts only what it needs.
    ///
    /// @return a builder pre-populated from this config
    public Builder toBuilder() {
        var b = new Builder(host, port);
        b.path = path;
        b.tls = tls;
        b.headers.putAll(headers);
        b.compression = compression;
        b.retry = retry;
        b.timeout = timeout;
        b.connectTimeout = connectTimeout;
        return b;
    }

    /// Translates this neutral config into the internal transport [ClientConfig]: the endpoint, path,
    /// TLS, headers, and compression map across directly, the [RetryPolicy] is carried as the
    /// client's neutral retry policy (the transport translates it internally), and the
    /// [TimeoutPolicy]'s per-attempt bound becomes the client's per-export deadline.
    ///
    /// @return the equivalent client config
    public ClientConfig toClientConfig() {
        var b = ClientConfig.builder()
                .setEndpoint(host, port)
                .setPath(path)
                .setTls(tls)
                .setHeaders(headers)
                .setCompression(compression)
                .setRetryPolicy(retry)
                .setTimeout(timeout.perAttempt());
        if (connectTimeout != null) {
            b.setConnectTimeout(connectTimeout);
        }
        return b.build();
    }

    /// A fluent builder for [OtlpExporterConfig], covering the endpoint and the common exporter knobs.
    public static final class Builder {

        private String host;
        private int port;
        private String path = "";
        private Tls tls = Tls.disabled();
        private final Map<String, String> headers = new LinkedHashMap<>();
        private Compression compression = Compression.NONE;
        private RetryPolicy retry = RetryPolicy.defaults();
        private TimeoutPolicy timeout = new TimeoutPolicy(Duration.ofSeconds(10), Duration.ofSeconds(10));
        private @Nullable Duration connectTimeout = null;

        private Builder(String host, int port) {
            this.host = host;
            this.port = port;
        }

        /// Sets the endpoint host and port.
        ///
        /// @param host the endpoint host
        /// @param port the endpoint port
        /// @return this builder
        public Builder setEndpoint(String host, int port) {
            this.host = host;
            this.port = port;
            return this;
        }

        /// Sets the OTLP/HTTP endpoint path prefix (ignored by gRPC).
        ///
        /// @param path the path prefix
        /// @return this builder
        public Builder setPath(String path) {
            this.path = path;
            return this;
        }

        /// Selects the TLS mode.
        ///
        /// @param tls the TLS mode
        /// @return this builder
        public Builder setTls(Tls tls) {
            this.tls = tls;
            return this;
        }

        /// Adds one request header sent on every export.
        ///
        /// @param key   the header name
        /// @param value the header value
        /// @return this builder
        public Builder addHeader(String key, String value) {
            this.headers.put(key, value);
            return this;
        }

        /// Selects request-body compression.
        ///
        /// @param compression the compression mode
        /// @return this builder
        public Builder setCompression(Compression compression) {
            this.compression = compression;
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

        /// Sets the neutral timeout policy.
        ///
        /// @param timeout the timeout policy
        /// @return this builder
        public Builder setTimeoutPolicy(TimeoutPolicy timeout) {
            this.timeout = timeout;
            return this;
        }

        /// Sets the optional OTLP/HTTP connection-setup bound.
        ///
        /// @param connectTimeout the connect timeout
        /// @return this builder
        public Builder setConnectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        /// Builds the config.
        ///
        /// @return the config
        public OtlpExporterConfig build() {
            return new OtlpExporterConfig(
                    host, port, path, tls, headers, compression, retry, timeout, connectTimeout);
        }
    }
}
