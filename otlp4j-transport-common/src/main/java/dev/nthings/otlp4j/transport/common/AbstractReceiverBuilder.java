package dev.nthings.otlp4j.transport.common;

import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.config.Tls;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.receiver.Receiver;
import dev.nthings.otlp4j.receiver.TelemetryTap;
import dev.nthings.otlp4j.spi.Dispatchers;
import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executor;
import org.jspecify.annotations.Nullable;

/// Shared builder base for OTLP receiver entry points. Holds the [ServerConfig.Builder] every
/// transport delegates to, the optional per-signal sinks, and the fluent setters common to all
/// transports; a concrete builder supplies the wire server via [#newServer] and adds
/// transport-specific knobs through [#config()]. [#build()] wraps the server in a
/// [ServerReceiver].
///
/// @param <B> the concrete builder type (self-type), so shared setters return the subtype and
///            fluent chains keep their transport-specific methods available
public abstract class AbstractReceiverBuilder<B extends AbstractReceiverBuilder<B>> {

    private ServerConfig.Builder config;
    private final String transportName;
    private @Nullable Sink<? super TracesData>   traces;
    private @Nullable Sink<? super MetricsData>  metrics;
    private @Nullable Sink<? super LogsData>     logs;
    private @Nullable Sink<? super ProfilesData> profiles;
    private GatewayMetrics gatewayMetrics = GatewayMetrics.noop();

    /// Seeds the base with the transport's default config (e.g. the HTTP entry point pre-sets
    /// port 4318) and the transport name used in log messages (e.g. `OTLP/gRPC`).
    ///
    /// @param config        the transport's pre-seeded config builder
    /// @param transportName the transport name used in log messages
    protected AbstractReceiverBuilder(ServerConfig.Builder config, String transportName) {
        this.config = Objects.requireNonNull(config, "config");
        this.transportName = Objects.requireNonNull(transportName, "transportName");
    }

    /// Returns `this`, typed as the concrete transport builder.
    ///
    /// @return this builder as the self-type
    protected abstract B self();

    /// Creates the wire server bound to the receiver's dispatch wiring.
    ///
    /// @param config      the finished transport config
    /// @param dispatchers the receiver's per-signal dispatch functions
    /// @param telemetry   the self-observability registry the server reports admission health to
    /// @return the transport server
    protected abstract OtlpServer newServer(
            ServerConfig config, Dispatchers dispatchers, GatewayMetrics telemetry);

    /// The live config builder, for a transport that needs to set a [ServerConfig] field directly.
    /// (The gRPC per-connection concurrency cap is threaded through the `GrpcOtlpServer`
    /// constructor instead, since it is not a shared config field.)
    ///
    /// @return the mutable config builder this base delegates to
    protected final ServerConfig.Builder config() {
        return config;
    }

    /// Replaces the whole server config at once; later setters override individual fields. The
    /// supplied values are used verbatim (transport defaults apply only to the unconfigured
    /// builder).
    ///
    /// @param config the replacement config
    /// @return this builder
    public final B setConfig(ServerConfig config) {
        this.config = config.toBuilder();
        return self();
    }

    /// Sets the bind host and port.
    ///
    /// @param bindHost the bind host
    /// @param port     the bind port
    /// @return this builder
    public final B setEndpoint(String bindHost, int port) {
        config.setBindHost(bindHost).setPort(port);
        return self();
    }

    /// Sets the bind port.
    ///
    /// @param port the bind port
    /// @return this builder
    public final B setPort(int port) {
        config.setPort(port);
        return self();
    }

    /// Binds an ephemeral port (port `0`); read the actual port from [Receiver#port()].
    ///
    /// @return this builder
    public final B ephemeralPort() {
        return setPort(0);
    }

    /// Selects the server TLS mode (e.g. [Tls#custom] to serve TLS). Defaults to plaintext.
    ///
    /// @param tls the TLS mode
    /// @return this builder
    public final B setTls(Tls tls) {
        config.setTls(tls);
        return self();
    }

    /// Caps a single decoded export request; guards against memory-exhausting oversized
    /// requests. Defaults to 4 MiB.
    ///
    /// @param bytes the per-request cap in bytes
    /// @return this builder
    public final B setMaxInboundMessageSizeBytes(int bytes) {
        config.setMaxInboundMessageSizeBytes(bytes);
        return self();
    }

    /// Caps decoded export requests in flight across every connection; excess requests are
    /// rejected retryably ("at capacity"). `0` (the default) disables the cap.
    ///
    /// @param max the global in-flight request cap
    /// @return this builder
    public final B setMaxInFlightRequests(int max) {
        config.setMaxInFlightRequests(max);
        return self();
    }

    /// Caps decoded request bytes in flight across every connection; excess requests are
    /// rejected retryably ("at capacity"). A request is always admitted when nothing else is in
    /// flight. `0` (the default) disables the cap.
    ///
    /// @param bytes the global in-flight byte cap
    /// @return this builder
    public final B setMaxInFlightBytes(long bytes) {
        config.setMaxInFlightBytes(bytes);
        return self();
    }

    /// Sets the retry-after hint attached to admission rejections (gRPC pushback trailer / HTTP
    /// `Retry-After`), telling throttled senders how long to wait. Unset (the default) emits no
    /// hint.
    ///
    /// @param throttleRetryAfter the hint attached to "at capacity" rejections
    /// @return this builder
    public final B setThrottleRetryAfter(Duration throttleRetryAfter) {
        config.setThrottleRetryAfter(throttleRetryAfter);
        return self();
    }

    /// Bounds the transport/TLS handshake deadline, not request body or connection idle time.
    /// Defaults to 20s.
    ///
    /// @param handshakeTimeout the handshake deadline
    /// @return this builder
    public final B setHandshakeTimeout(Duration handshakeTimeout) {
        config.setHandshakeTimeout(handshakeTimeout);
        return self();
    }

    /// Supplies the executor that runs admitted requests; a bounded pool caps concurrent work.
    /// Defaults to the transport's own executor.
    ///
    /// The pool and admission control compose along different axes: a bounded `serverExecutor` is
    /// the hard concurrency cap for handler threads, while admission independently caps decoded
    /// in-flight requests/bytes. A request blocked waiting for a pool thread holds no admission
    /// permit yet (the permit is acquired inside the handler, after the pool hands off), so a
    /// bounded pool smaller than the offered load queues exchanges at the pool, not at admission.
    ///
    /// @param serverExecutor the request executor
    /// @return this builder
    public final B setServerExecutor(Executor serverExecutor) {
        config.setServerExecutor(serverExecutor);
        return self();
    }

    /// Attaches the traces sink the receiver dispatches to.
    ///
    /// @param sink the traces sink
    /// @return this builder
    public final B onTraces(Sink<? super TracesData> sink) {
        this.traces = sink;
        return self();
    }

    /// Attaches the metrics sink the receiver dispatches to.
    ///
    /// @param sink the metrics sink
    /// @return this builder
    public final B onMetrics(Sink<? super MetricsData> sink) {
        this.metrics = sink;
        return self();
    }

    /// Attaches the logs sink the receiver dispatches to.
    ///
    /// @param sink the logs sink
    /// @return this builder
    public final B onLogs(Sink<? super LogsData> sink) {
        this.logs = sink;
        return self();
    }

    /// Attaches the profiles sink the receiver dispatches to.
    ///
    /// @param sink the profiles sink
    /// @return this builder
    public final B onProfiles(Sink<? super ProfilesData> sink) {
        this.profiles = sink;
        return self();
    }

    /// Sets the [GatewayMetrics] registry the receiver reports to (default [GatewayMetrics#noop]).
    /// Named `telemetry` rather than `metrics` because the per-signal OTLP metrics sink already owns
    /// [#onMetrics]; this knob is the gateway's self-observability, feeding the admission and tap
    /// instruments. Threaded into the transport server (admission health) and the [TelemetryTap]
    /// (`otlp4j.receiver.tap.dropped`).
    ///
    /// @param telemetry the self-observability registry
    /// @return this builder
    public final B telemetry(GatewayMetrics telemetry) {
        this.gatewayMetrics = Objects.requireNonNull(telemetry, "telemetry");
        return self();
    }

    /// Builds the receiver: finishes the config, asks the transport for its server, and wraps it
    /// in a [ServerReceiver] with any attached sinks subscribed.
    ///
    /// @return the receiver; call [Receiver#start()] to bind the transport
    public final Receiver build() {
        var built = config.build();
        return new ServerReceiver(transportName, disp -> newServer(built, disp, gatewayMetrics),
                traces, metrics, logs, profiles, gatewayMetrics);
    }
}
