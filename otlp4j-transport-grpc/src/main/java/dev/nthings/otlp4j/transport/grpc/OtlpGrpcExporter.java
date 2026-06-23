package dev.nthings.otlp4j.transport.grpc;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.config.RetryPolicy;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.exporter.AbstractOtlpExporter;
import dev.nthings.otlp4j.transport.grpc.internal.GrpcOtlpClient;
import java.time.Duration;
import java.util.Map;

/// Exports typed telemetry to an OTLP/gRPC endpoint.
///
/// One instance handles all four signals via the per-signal facets ([#traces()], [#metrics()],
/// [#logs()], [#profiles()]); lifecycle (flush, drain, shutdown) is inherited from
/// [AbstractOtlpExporter]. The default endpoint is `localhost:4317`.
public final class OtlpGrpcExporter extends AbstractOtlpExporter {

    private OtlpGrpcExporter(Builder b) {
        super(new GrpcOtlpClient(b.config.build()));
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Convenience constructor: build, connect, ready-to-use.
    public static OtlpGrpcExporter to(String host, int port) {
        return builder().endpoint(host, port).build();
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

        /// Selects the client TLS mode (e.g. [Tls#systemTrust()] or [Tls#custom]). Defaults to
        /// plaintext.
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

        public OtlpGrpcExporter build() {
            return new OtlpGrpcExporter(this);
        }
    }
}
