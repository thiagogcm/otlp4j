package dev.nthings.otlp4j.transport.grpc;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.config.RetryPolicy;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.exporter.OtlpExporter;
import dev.nthings.otlp4j.transport.spi.ClientExporter;
import dev.nthings.otlp4j.transport.grpc.internal.GrpcOtlpClient;
import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;

/// Builds [OtlpExporter]s that send typed telemetry to an OTLP/gRPC endpoint.
///
/// One exporter handles all four signals via per-signal facets; flush/drain/shutdown live on the
/// returned [OtlpExporter]. The default endpoint is `localhost:4317`.
public final class OtlpGrpcExporter {

    private OtlpGrpcExporter() {}

    public static Builder builder() {
        return new Builder();
    }

    /// Convenience factory: builds and returns a ready-to-use [OtlpExporter].
    public static OtlpExporter to(String host, int port) {
        return builder().setEndpoint(host, port).build();
    }

    /// Builds from the standard `OTEL_EXPORTER_OTLP_*` variables (see [Builder#fromEnvironment()]).
    /// Equivalent to `builder().fromEnvironment().build()`.
    public static OtlpExporter fromEnvironment() {
        return builder().fromEnvironment().build();
    }

    /// Builder for [OtlpGrpcExporter]. Defaults to `localhost:4317` with a 10s deadline.
    public static final class Builder {

        private ClientConfig.Builder config = ClientConfig.builder();

        private Builder() {}

        public Builder transport(ClientConfig config) {
            this.config = config.toBuilder();
            return this;
        }

        /// Applies the standard `OTEL_EXPORTER_OTLP_*` variables (see
        /// [ClientConfig.Builder#fromEnvironment()]). Opt-in; environment values are lowest
        /// precedence, so explicit setters win regardless of call order.
        public Builder fromEnvironment() {
            config.fromEnvironment();
            return this;
        }

        /// Sets the endpoint from a `scheme://host:port[/prefix]` URL; `https` selects system-trust TLS.
        public Builder setEndpoint(String url) {
            config.setEndpoint(url);
            return this;
        }

        public Builder setEndpoint(String host, int port) {
            config.setEndpoint(host, port);
            return this;
        }

        public Builder setHost(String host) {
            config.setHost(host);
            return this;
        }

        public Builder setPort(int port) {
            config.setPort(port);
            return this;
        }

        public Builder setTimeout(Duration timeout) {
            config.setTimeout(timeout);
            return this;
        }

        /// Selects the client TLS mode (e.g. [Tls#systemTrust()] or [Tls.Custom]). Defaults to
        /// no TLS (plaintext).
        public Builder setTls(Tls tls) {
            config.setTls(tls);
            return this;
        }

        /// Adds one request metadata header (e.g. `authorization`) sent on every export.
        public Builder addHeader(String key, String value) {
            config.addHeader(key, value);
            return this;
        }

        /// Replaces any existing headers with the supplied map.
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

        /// Selects request-body compression (e.g. [Compression#GZIP]). Defaults to none.
        public Builder setCompression(Compression compression) {
            config.setCompression(compression);
            return this;
        }

        /// Familiar string door for compression: `gzip` or `none`.
        public Builder setCompression(String compression) {
            config.setCompression(compression);
            return this;
        }

        /// Sets the transport retry policy (e.g. [RetryPolicy#builder()]). Defaults to
        /// [RetryPolicy#getDefault()]; pass [RetryPolicy#none()] to disable retries.
        public Builder setRetryPolicy(RetryPolicy retry) {
            config.setRetryPolicy(retry);
            return this;
        }

        public OtlpExporter build() {
            return new ClientExporter(new GrpcOtlpClient(config.build()), OtlpGrpcExporter.class.getName());
        }
    }
}
