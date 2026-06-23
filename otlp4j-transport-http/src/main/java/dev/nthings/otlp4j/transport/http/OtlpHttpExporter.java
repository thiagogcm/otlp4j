package dev.nthings.otlp4j.transport.http;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.config.RetryPolicy;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.exporter.AbstractOtlpExporter;
import dev.nthings.otlp4j.transport.http.internal.HttpOtlpClient;
import java.time.Duration;
import java.util.Map;

/// Exports typed telemetry to an OTLP/HTTP endpoint using binary-protobuf request bodies
/// (`Content-Type: application/x-protobuf`).
///
/// One instance handles all four signals via the per-signal facets ([#traces()], [#metrics()],
/// [#logs()], [#profiles()]); lifecycle (flush, drain, shutdown) is inherited from
/// [AbstractOtlpExporter].
///
/// Each signal is POSTed to its standard path under the configured `host:port` —
/// `/v1/traces`, `/v1/metrics`, `/v1/logs`, and `/v1development/profiles` — with the scheme chosen
/// by [Tls] (`http` when disabled, otherwise `https`). The default endpoint is `localhost:4318`.
public final class OtlpHttpExporter extends AbstractOtlpExporter {

    /// The conventional OTLP/HTTP port, used as the builder default (gRPC uses 4317).
    static final int DEFAULT_HTTP_PORT = 4318;

    private OtlpHttpExporter(Builder b) {
        super(new HttpOtlpClient(b.config.build()));
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Convenience constructor: build, connect, ready-to-use.
    public static OtlpHttpExporter to(String host, int port) {
        return builder().endpoint(host, port).build();
    }

    /// Builder for [OtlpHttpExporter]. Defaults to `localhost:4318` with a 10s deadline.
    public static final class Builder {

        private ClientConfig.Builder config =
                ClientConfig.builder().port(DEFAULT_HTTP_PORT);

        private Builder() {}

        /// Replaces the whole transport config. The supplied config's port is used verbatim (the
        /// 4318 HTTP default applies only to the unconfigured builder).
        public Builder transport(ClientConfig config) {
            this.config = config.toBuilder();
            return this;
        }

        /// Applies the standard `OTEL_EXPORTER_OTLP_*` variables (see
        /// [ClientConfig.Builder#fromEnvironment()]). Opt-in; call it first so explicit
        /// setters override the environment. An endpoint URL without a port keeps the 4318 default.
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

        /// Selects the client TLS mode (e.g. [Tls#systemTrust()] or [Tls#custom]); also chooses the
        /// `http` vs `https` scheme. Defaults to plaintext `http`.
        public Builder tls(Tls tls) {
            config.tls(tls);
            return this;
        }

        /// Adds one HTTP request header (e.g. `authorization`) sent on every export.
        public Builder header(String key, String value) {
            config.header(key, value);
            return this;
        }

        /// Adds all of `headers` as HTTP request headers, on top of any already set.
        public Builder headers(Map<String, String> headers) {
            config.headers(headers);
            return this;
        }

        /// Selects request-body compression (e.g. [Compression#GZIP], sent as
        /// `Content-Encoding: gzip`). Defaults to none.
        public Builder compression(Compression compression) {
            config.compression(compression);
            return this;
        }

        /// Sets the transport retry policy (e.g. [RetryPolicy#exponential]). Defaults to no retries.
        public Builder retry(RetryPolicy retry) {
            config.retry(retry);
            return this;
        }

        public OtlpHttpExporter build() {
            return new OtlpHttpExporter(this);
        }
    }
}
