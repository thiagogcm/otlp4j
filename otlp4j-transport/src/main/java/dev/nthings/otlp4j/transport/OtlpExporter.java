package dev.nthings.otlp4j.transport;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import dev.nthings.otlp4j.transport.internal.ArmeriaOtlpClient;
import dev.nthings.otlp4j.transport.internal.ExporterMetrics;
import dev.nthings.otlp4j.transport.internal.Protocol;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/// Builds [ClientExporter]s that send typed telemetry to an OTLP endpoint over Armeria.
///
/// One exporter handles all four signals via per-signal facets; flush/drain/shutdown live on the
/// returned [ClientExporter]. [#grpcBuilder()] speaks OTLP/gRPC (default `localhost:4317`),
/// [#httpBuilder()] speaks OTLP/HTTP (default `localhost:4318`, POSTing each signal to its standard
/// `/v1/...` path). The scheme is chosen by the configured TLS mode.
public final class OtlpExporter {

    private OtlpExporter() {}

    /// A builder speaking OTLP/gRPC, default endpoint `localhost:4317`.
    ///
    /// @return the gRPC exporter builder
    public static Builder grpcBuilder() {
        return new Builder(Protocol.GRPC, OtlpExporterConfig.builder(4317),
                OtlpExporter.class.getName() + "/gRPC");
    }

    /// A builder speaking OTLP/HTTP, default endpoint `localhost:4318`.
    ///
    /// @return the HTTP exporter builder
    public static Builder httpBuilder() {
        return new Builder(Protocol.HTTP, OtlpExporterConfig.builder(4318),
                OtlpExporter.class.getName() + "/HTTP");
    }

    /// Builder for an OTLP exporter. The wire knobs are held on an [OtlpExporterConfig.Builder] and
    /// build the same [OtlpExporterConfig] the graph factory consumes; [#build()] wires the built
    /// config into an [ArmeriaOtlpClient] and wraps it in a [ClientExporter]. The path prefix is
    /// honoured by OTLP/HTTP and ignored by OTLP/gRPC.
    public static final class Builder {

        private final Protocol protocol;
        private OtlpExporterConfig.Builder config;
        private final String exporterType;
        private ComponentId id = ComponentId.parse("otlp");
        private GatewayMetrics gatewayMetrics = GatewayMetrics.noop();

        private Builder(Protocol protocol, OtlpExporterConfig.Builder config, String exporterType) {
            this.protocol = protocol;
            this.config = Objects.requireNonNull(config, "config");
            this.exporterType = Objects.requireNonNull(exporterType, "exporterType");
        }

        /// Replaces the whole exporter config at once; later setters override individual fields.
        ///
        /// @param config the replacement config
        /// @return this builder
        public Builder setConfig(OtlpExporterConfig config) {
            this.config = config.toBuilder();
            return this;
        }

        /// Applies the standard `OTEL_EXPORTER_OTLP_*` variables (see
        /// [OtlpExporterConfig.Builder#fromEnvironment()]). Opt-in; environment values are lowest
        /// precedence, so explicit setters win regardless of call order.
        ///
        /// @return this builder
        public Builder fromEnvironment() {
            config.fromEnvironment();
            return this;
        }

        /// Sets the endpoint from a `scheme://host:port[/prefix]` URL; `https` selects system-trust
        /// TLS, and any path becomes the endpoint path prefix (honoured by OTLP/HTTP, ignored by
        /// OTLP/gRPC).
        ///
        /// @param url the endpoint URL
        /// @return this builder
        public Builder setEndpoint(String url) {
            config.setEndpoint(url);
            return this;
        }

        /// Sets the endpoint host and port.
        ///
        /// @param host the endpoint host
        /// @param port the endpoint port
        /// @return this builder
        public Builder setEndpoint(String host, int port) {
            config.setEndpoint(host, port);
            return this;
        }

        /// Sets an endpoint path prefix (e.g. `/otlp`) prepended to the per-signal paths; blank or `/`
        /// mean none. Honoured by OTLP/HTTP, ignored by OTLP/gRPC.
        ///
        /// @param path the endpoint path prefix
        /// @return this builder
        public Builder setPath(String path) {
            config.setPath(path);
            return this;
        }

        /// Bounds a single export attempt end to end. Unless a larger total is set through
        /// [#setTotalTimeout], this is also the total budget.
        ///
        /// @param timeout the per-attempt deadline
        /// @return this builder
        public Builder setTimeout(Duration timeout) {
            config.setTimeout(timeout);
            return this;
        }

        /// Bounds the whole retrying delivery end to end.
        ///
        /// @param totalTimeout the total budget across all attempts
        /// @return this builder
        public Builder setTotalTimeout(Duration totalTimeout) {
            config.setTotalTimeout(totalTimeout);
            return this;
        }

        /// Selects the client TLS mode (e.g. [Tls#systemTrust()] or [Tls.Custom]); for OTLP/HTTP it
        /// also chooses the `http` vs `https` scheme. Defaults to plaintext.
        ///
        /// @param tls the TLS mode
        /// @return this builder
        public Builder setTls(Tls tls) {
            config.setTls(tls);
            return this;
        }

        /// Adds one request header (e.g. `authorization`) sent on every export.
        ///
        /// @param key   the header name
        /// @param value the header value
        /// @return this builder
        public Builder addHeader(String key, String value) {
            config.addHeader(key, value);
            return this;
        }

        /// Replaces any existing headers with the supplied map.
        ///
        /// @param headers the replacement headers
        /// @return this builder
        public Builder setHeaders(Map<String, String> headers) {
            config.setHeaders(headers);
            return this;
        }

        /// Sets a per-export header supplier (e.g. a rotating bearer token), re-evaluated on every
        /// export and overlaid on the static headers.
        ///
        /// @param headerSupplier the per-export header supplier
        /// @return this builder
        public Builder setHeaders(Supplier<Map<String, String>> headerSupplier) {
            config.setHeaders(headerSupplier);
            return this;
        }

        /// Selects request-body compression (e.g. [Compression#GZIP]). Defaults to none.
        ///
        /// @param compression the compression mode
        /// @return this builder
        public Builder setCompression(Compression compression) {
            config.setCompression(compression);
            return this;
        }

        /// Familiar string door for compression: `gzip` or `none`.
        ///
        /// @param compression the compression name
        /// @return this builder
        public Builder setCompression(String compression) {
            config.setCompression(compression);
            return this;
        }

        /// Sets the neutral retry policy; pass a policy with `maxAttempts` of `1` to disable retries.
        ///
        /// @param retry the retry policy
        /// @return this builder
        public Builder setRetryPolicy(RetryPolicy retry) {
            config.setRetryPolicy(retry);
            return this;
        }

        /// Sets the [GatewayMetrics] registry the exporter reports to (default [GatewayMetrics#noop]).
        ///
        /// @param telemetry the self-observability registry
        /// @return this builder
        public Builder telemetry(GatewayMetrics telemetry) {
            this.gatewayMetrics = Objects.requireNonNull(telemetry, "telemetry");
            return this;
        }

        /// Sets the exporter component identity, attributed to every delivery result. Defaults to the
        /// `otlp` type with no name.
        ///
        /// @param id the exporter component identity
        /// @return this builder
        public Builder componentId(ComponentId id) {
            this.id = Objects.requireNonNull(id, "id");
            return this;
        }

        /// Builds the exporter: finishes the config, opens the wire client, and wraps it in a
        /// [ClientExporter].
        ///
        /// @return the ready-to-use exporter
        public ClientExporter build() {
            var metrics = ExporterMetrics.from(gatewayMetrics);
            var client = new ArmeriaOtlpClient(config.build(), metrics, protocol);
            return new ClientExporter(client, id, exporterType, metrics);
        }
    }
}
