package dev.nthings.otlp4j.transport.http;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.config.RetryPolicy;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.exporter.OtlpExporter;
import dev.nthings.otlp4j.transport.spi.ClientExporter;
import dev.nthings.otlp4j.transport.http.internal.HttpOtlpClient;
import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;

/// Builds [OtlpExporter]s that send typed telemetry to an OTLP/HTTP endpoint.
///
/// One exporter handles all four signals via per-signal facets; flush/drain/shutdown live on the
/// returned [OtlpExporter]. Each signal is POSTed to its standard path
/// (`/v1/traces`, `/v1/metrics`, `/v1/logs`, `/v1development/profiles`) under the configured
/// `host:port`, with the scheme chosen by [Tls]. The default endpoint is `localhost:4318`.
public final class OtlpHttpExporter {

    /// The conventional OTLP/HTTP port (4318); gRPC uses 4317.
    static final int DEFAULT_HTTP_PORT = 4318;

    private OtlpHttpExporter() {}

    public static Builder builder() {
        return new Builder();
    }

    /// Convenience factory: builds and returns a ready-to-use [OtlpExporter].
    public static OtlpExporter to(String host, int port) {
        return builder().setEndpoint(host, port).build();
    }

    /// Builds from the standard `OTEL_EXPORTER_OTLP_*` variables.
    /// Equivalent to `builder().fromEnvironment().build()`.
    public static OtlpExporter fromEnvironment() {
        return builder().fromEnvironment().build();
    }

    /// Builder for [OtlpHttpExporter]. Defaults to `localhost:4318` with a 10s deadline.
    public static final class Builder {

        private ClientConfig.Builder config =
                ClientConfig.builder().setPort(DEFAULT_HTTP_PORT);

        private Builder() {}

        /// Replaces the whole transport config. The supplied port is used verbatim (the 4318
        /// default applies only to the unconfigured builder).
        public Builder setConfig(ClientConfig config) {
            this.config = config.toBuilder();
            return this;
        }

        /// Applies the standard `OTEL_EXPORTER_OTLP_*` variables. Opt-in; environment values are
        /// lowest precedence, so explicit setters win regardless of call order.
        public Builder fromEnvironment() {
            config.fromEnvironment();
            return this;
        }

        /// Sets the endpoint from a `scheme://host:port[/prefix]` URL; `https` selects system-trust
        /// TLS and any path becomes the endpoint path prefix.
        public Builder setEndpoint(String url) {
            config.setEndpoint(url);
            return this;
        }

        public Builder setEndpoint(String host, int port) {
            config.setEndpoint(host, port);
            return this;
        }

        /// Sets an endpoint path prefix (e.g. `/otlp`) prepended to the per-signal paths; blank or
        /// `/` mean none.
        public Builder setPath(String path) {
            config.setPath(path);
            return this;
        }

        public Builder setTimeout(Duration timeout) {
            config.setTimeout(timeout);
            return this;
        }

        /// Bounds connection setup; unset falls back to [#setTimeout].
        public Builder setConnectTimeout(Duration connectTimeout) {
            config.setConnectTimeout(connectTimeout);
            return this;
        }

        /// Selects the client TLS mode (e.g. [Tls#systemTrust()] or [Tls.Custom]); also chooses the
        /// `http` vs `https` scheme. Defaults to plaintext `http`.
        public Builder setTls(Tls tls) {
            config.setTls(tls);
            return this;
        }

        /// Adds one HTTP request header (e.g. `authorization`) sent on every export.
        public Builder addHeader(String key, String value) {
            config.addHeader(key, value);
            return this;
        }

        /// Replaces any existing HTTP request headers with the supplied map.
        public Builder setHeaders(Map<String, String> headers) {
            config.setHeaders(headers);
            return this;
        }

        /// Sets a per-export header supplier (e.g. a rotating bearer token), re-evaluated on every
        /// export and overlaid on the static headers.
        public Builder setHeaders(Supplier<Map<String, String>> headerSupplier) {
            config.setHeaders(headerSupplier);
            return this;
        }

        /// Selects request-body compression (e.g. [Compression#GZIP] sent as
        /// `Content-Encoding: gzip`). Defaults to none.
        public Builder setCompression(Compression compression) {
            config.setCompression(compression);
            return this;
        }

        /// Familiar string door for compression: `gzip` or `none`.
        public Builder setCompression(String compression) {
            config.setCompression(compression);
            return this;
        }

        /// Sets the transport retry policy. Defaults to [RetryPolicy#getDefault()]; pass
        /// [RetryPolicy#none()] to disable retries.
        public Builder setRetryPolicy(RetryPolicy retry) {
            config.setRetryPolicy(retry);
            return this;
        }

        public OtlpExporter build() {
            return new ClientExporter(new HttpOtlpClient(config.build()), OtlpHttpExporter.class.getName());
        }
    }
}
