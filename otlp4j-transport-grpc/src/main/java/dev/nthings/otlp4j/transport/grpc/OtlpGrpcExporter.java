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
        return builder().endpoint(host, port).build();
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
        /// [ClientConfig.Builder#fromEnvironment()]). Opt-in; call it first so explicit
        /// setters override the environment.
        public Builder fromEnvironment() {
            config.fromEnvironment();
            return this;
        }

        public Builder endpoint(String host, int port) {
            config.endpoint(host, port);
            return this;
        }

        public Builder host(String host) {
            config.host(host);
            return this;
        }

        public Builder port(int port) {
            config.port(port);
            return this;
        }

        public Builder timeout(Duration timeout) {
            config.timeout(timeout);
            return this;
        }

    /// Selects the client TLS mode (e.g. [Tls#systemTrust()] or [Tls.Custom]). Defaults to
    /// no TLS (plaintext).
        public Builder tls(Tls tls) {
            config.tls(tls);
            return this;
        }

        /// Adds one request metadata header (e.g. `authorization`) sent on every export.
        public Builder header(String key, String value) {
            config.header(key, value);
            return this;
        }

        /// Replaces any existing headers with the supplied map.
        public Builder headers(Map<String, String> headers) {
            config.headers(headers);
            return this;
        }

        /// Adds all of `headers` as request metadata, on top of any already set.
        public Builder addHeaders(Map<String, String> headers) {
            config.addHeaders(headers);
            return this;
        }

        /// Selects request-body compression (e.g. [Compression#GZIP]). Defaults to none.
        public Builder compression(Compression compression) {
            config.compression(compression);
            return this;
        }

        /// Sets the transport retry policy (e.g. [RetryPolicy#exponential]). Defaults to no retries.
        public Builder retry(RetryPolicy retry) {
            config.retry(retry);
            return this;
        }

        public OtlpExporter build() {
            return new ClientExporter(new GrpcOtlpClient(config.build()), OtlpGrpcExporter.class.getName());
        }
    }
}
