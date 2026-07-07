package dev.nthings.otlp4j.transport.common;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.spi.Dispatchers;
import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// The transport-neutral receiver runtime backed by a transport [OtlpServer]. It holds one
/// [SignalSource] per signal, routes each decoded [TelemetryPayload] the server hands it into the
/// graph consumer attached for that signal, and returns the graph's [DeliveryResult] to the transport
/// verbatim. Alongside routing it mirrors every accepted payload to a live [ReceiverTap] side-channel,
/// which never back-pressures the acknowledgement path and feeds the `otlp4j.receiver.tap.dropped`
/// counter. gRPC and HTTP entry points compose one of these through their receiver builders, and the
/// graph reaches it through a `ReceiverComponentAdapter`.
public final class ServerReceiver implements Dispatchers {

    private static final Logger log = LoggerFactory.getLogger(ServerReceiver.class);

    private final Map<SignalDescriptor<?>, SignalSource<?>> sources = new LinkedHashMap<>();
    private final ReceiverTap tap;
    private final OtlpServer server;
    private final String transportName;
    private final Clock clock;

    /// Builds the per-signal routing and the telemetry tap, attaches the routed graph consumers, then
    /// asks `serverFactory` for the transport server bound to it. `transportName` (e.g. `OTLP/gRPC`) is
    /// used only for log messages; `id` attributes the no-consumer rejections; `clock` stamps each
    /// consume deadline; `telemetry` feeds the tap's `otlp4j.receiver.tap.dropped` counter.
    ///
    /// @param transportName the transport name used in log messages
    /// @param id            the receiver component identity, attributed to no-consumer rejections
    /// @param clock         the clock stamping each consume deadline
    /// @param telemetry     the self-observability registry the tap's drop counter reports to
    /// @param serverFactory builds the transport server bound to this dispatcher
    /// @param routes        the routed graph consumers, keyed by the signal each serves
    public ServerReceiver(
            String transportName,
            ComponentId id,
            Clock clock,
            GatewayMetrics telemetry,
            Function<Dispatchers, OtlpServer> serverFactory,
            Map<SignalDescriptor<?>, SignalConsumer<TelemetryPayload>> routes) {
        this.transportName = transportName;
        this.clock = clock;
        this.tap = new ReceiverTap(telemetry);
        for (SignalDescriptor<?> signal : SignalDescriptor.values()) {
            sources.put(signal, new SignalSource<>(signal, id));
        }
        routes.forEach(this::attach);
        this.server = serverFactory.apply(this);
    }

    private <T> SignalSource<T> sourceFor(SignalDescriptor<T> signal) {
        // The map is keyed by SignalDescriptor identity and each entry was created for that signal,
        // so the value's batch type is exactly the signal's batch type.
        @SuppressWarnings("unchecked")
        var source = (SignalSource<T>) sources.get(signal);
        return source;
    }

    private void attach(SignalDescriptor<?> signal, SignalConsumer<TelemetryPayload> consumer) {
        sourceFor(signal).attach(consumer);
    }

    /// Starts the underlying transport; throws [UncheckedIOException] if the bind fails.
    ///
    /// @return this receiver
    public ServerReceiver start() {
        try {
            server.start();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to start " + transportName + " receiver", e);
        }
        log.debug("{} receiver started on port {}", transportName, server.port());
        return this;
    }

    /// The port the underlying transport is bound to; 0 before [#start] completes.
    ///
    /// @return the bound port
    public int port() {
        return server.port();
    }

    /// Drains in-flight requests and stops the transport within `timeout`, then closes the tap. The
    /// tap is closed after the drain so payloads accepted while draining are still mirrored.
    ///
    /// @param timeout the drain deadline
    /// @return a stage that completes when teardown finishes
    public CompletionStage<Void> shutdown(Duration timeout) {
        return server.shutdown(timeout).whenComplete((v, t) -> tap.close());
    }

    /// Forcibly stops the transport, returning promptly. Nothing further is accepted, so the tap is
    /// detached immediately.
    ///
    /// @return a stage that completes when the forceful teardown finishes
    public CompletionStage<Void> shutdownNow() {
        tap.close();
        return server.shutdownNow();
    }

    @Override
    public CompletionStage<DeliveryResult> dispatch(TelemetryPayload payload) {
        // Mirror to the tap before routing, so a side-channel observer sees exactly what is admitted;
        // the tap never back-pressures, so it cannot delay the acknowledgement the consumer produces.
        tap.publish(payload);
        return sourceFor(payload.signal()).dispatch(new ReceiverConsumeContext(clock), payload);
    }
}
