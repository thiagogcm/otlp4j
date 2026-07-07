package dev.nthings.otlp4j.transport.common;

import dev.nthings.otlp4j.config.ClientConfig;
import dev.nthings.otlp4j.config.Compression;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/// Shared builder base for OTLP exporter entry points. Holds the [ClientConfig.Builder] every
/// transport delegates to and the fluent setters common to all transports; a concrete builder
/// supplies the wire client via [#newClient] and adds transport-specific knobs through
/// [#config()]. [#build()] wraps the client in a [ClientExporter].
///
/// @param <B> the concrete builder type (self-type), so shared setters return the subtype and
///            fluent chains keep their transport-specific methods available
public abstract class AbstractExporterBuilder<B extends AbstractExporterBuilder<B>> {

    private ClientConfig.Builder config;
    private final String exporterType;
    private ComponentId id = ComponentId.parse("otlp");
    private GatewayMetrics gatewayMetrics = GatewayMetrics.noop();

    /// Seeds the base with the transport's default config (e.g. the HTTP entry point pre-sets
    /// port 4318) and the exporter type name reported by the lifecycle leak warning.
    ///
    /// @param config       the transport's pre-seeded config builder
    /// @param exporterType the exporter name used in the leak warning
    protected AbstractExporterBuilder(ClientConfig.Builder config, String exporterType) {
        this.config = Objects.requireNonNull(config, "config");
        this.exporterType = Objects.requireNonNull(exporterType, "exporterType");
    }

    /// Returns `this`, typed as the concrete transport builder.
    ///
    /// @return this builder as the self-type
    protected abstract B self();

    /// Creates the wire client the built exporter delegates to.
    ///
    /// @param config the finished transport config
    /// @return the transport client
    protected abstract OtlpClient newClient(ClientConfig config);

    /// The live config builder, for transport-specific setters (e.g. the HTTP `setPath`).
    ///
    /// @return the mutable config builder this base delegates to
    protected final ClientConfig.Builder config() {
        return config;
    }

    /// Replaces the whole client config at once; later setters override individual fields. The
    /// supplied values are used verbatim (transport defaults apply only to the unconfigured
    /// builder).
    ///
    /// @param config the replacement config
    /// @return this builder
    public final B setConfig(ClientConfig config) {
        this.config = config.toBuilder();
        return self();
    }

    /// Applies the standard `OTEL_EXPORTER_OTLP_*` variables (see
    /// [ClientConfig.Builder#fromEnvironment()]). Opt-in; environment values are lowest
    /// precedence, so explicit setters win regardless of call order.
    ///
    /// @return this builder
    public final B fromEnvironment() {
        config.fromEnvironment();
        return self();
    }

    /// Sets the endpoint from a `scheme://host:port[/prefix]` URL; `https` selects system-trust
    /// TLS, and any path becomes the endpoint path prefix (honoured by OTLP/HTTP, ignored by
    /// OTLP/gRPC).
    ///
    /// @param url the endpoint URL
    /// @return this builder
    public final B setEndpoint(String url) {
        config.setEndpoint(url);
        return self();
    }

    /// Sets the endpoint host and port.
    ///
    /// @param host the endpoint host
    /// @param port the endpoint port
    /// @return this builder
    public final B setEndpoint(String host, int port) {
        config.setEndpoint(host, port);
        return self();
    }

    /// Bounds a single export call end to end.
    ///
    /// @param timeout the per-export deadline
    /// @return this builder
    public final B setTimeout(Duration timeout) {
        config.setTimeout(timeout);
        return self();
    }

    /// Selects the client TLS mode (e.g. [Tls#systemTrust()] or [Tls.Custom]); for OTLP/HTTP it
    /// also chooses the `http` vs `https` scheme. Defaults to plaintext.
    ///
    /// @param tls the TLS mode
    /// @return this builder
    public final B setTls(Tls tls) {
        config.setTls(tls);
        return self();
    }

    /// Adds one request header (e.g. `authorization`) sent on every export.
    ///
    /// @param key   the header name
    /// @param value the header value
    /// @return this builder
    public final B addHeader(String key, String value) {
        config.addHeader(key, value);
        return self();
    }

    /// Replaces any existing headers with the supplied map.
    ///
    /// @param headers the replacement headers
    /// @return this builder
    public final B setHeaders(Map<String, String> headers) {
        config.setHeaders(headers);
        return self();
    }

    /// Sets a per-export header supplier (e.g. a rotating bearer token), re-evaluated on every
    /// export and overlaid on the static headers.
    ///
    /// @param headerSupplier the per-export header supplier
    /// @return this builder
    public final B setHeaders(Supplier<Map<String, String>> headerSupplier) {
        config.setHeaders(headerSupplier);
        return self();
    }

    /// Selects request-body compression (e.g. [Compression#GZIP]). Defaults to none.
    ///
    /// @param compression the compression mode
    /// @return this builder
    public final B setCompression(Compression compression) {
        config.setCompression(compression);
        return self();
    }

    /// Familiar string door for compression: `gzip` or `none`.
    ///
    /// @param compression the compression name
    /// @return this builder
    public final B setCompression(String compression) {
        config.setCompression(compression);
        return self();
    }

    /// Sets the neutral retry policy. Defaults to [ClientConfig#defaultRetryPolicy()]; pass a policy
    /// with `maxAttempts` of `1` to disable retries. The transport translates this policy to its
    /// internal resilience library.
    ///
    /// @param retry the retry policy
    /// @return this builder
    public final B setRetryPolicy(RetryPolicy retry) {
        config.setRetryPolicy(retry);
        return self();
    }

    /// Sets the [GatewayMetrics] registry the exporter reports to (default [GatewayMetrics#noop]).
    /// The exporter feeds the `otlp4j.exporter.export.success` / `otlp4j.exporter.export.failure`
    /// counters off each export's outcome. Named `telemetry` for vocabulary parity with the receiver
    /// and batcher knobs.
    ///
    /// @param telemetry the self-observability registry
    /// @return this builder
    public final B telemetry(GatewayMetrics telemetry) {
        this.gatewayMetrics = Objects.requireNonNull(telemetry, "telemetry");
        return self();
    }

    /// Sets the exporter component identity, attributed to every delivery result. Defaults to the
    /// `otlp` type with no name.
    ///
    /// @param id the exporter component identity
    /// @return this builder
    public final B componentId(ComponentId id) {
        this.id = Objects.requireNonNull(id, "id");
        return self();
    }

    /// Builds the exporter: finishes the config, asks the transport for its client, and wraps it
    /// in a [ClientExporter].
    ///
    /// @return the ready-to-use exporter
    public final ClientExporter build() {
        return new ClientExporter(newClient(config.build()), id, exporterType, gatewayMetrics);
    }
}
