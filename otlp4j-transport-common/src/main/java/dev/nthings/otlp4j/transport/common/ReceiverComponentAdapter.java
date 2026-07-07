package dev.nthings.otlp4j.transport.common;

import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.NetworkBound;
import dev.nthings.otlp4j.core.component.Receiver;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.consumer.SignalConsumers;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/// Presents a transport [ServerReceiver] as a core graph [Receiver].
///
/// The graph hands a [SignalConsumers] bundle to the receiver factory; this adapter routes each of
/// the receiver's served signals onto the matching graph consumer through the transport builder, then
/// builds the [ServerReceiver]. The receiver speaks
/// [dev.nthings.otlp4j.core.result.DeliveryResult] natively, so no result bridge is needed. The
/// adapter also implements [NetworkBound], delegating the bound port to the receiver, so the runtime
/// component lookup can read a graph-booted receiver's ephemeral port.
///
/// The lifecycle maps directly: [#start(ComponentContext)] binds the transport and
/// [#shutdown(ShutdownContext)] drains it within the deadline. A receiver buffers nothing, so
/// `forceFlush` keeps the no-op default.
///
/// The transport bind ([ServerReceiver#start()]) is a blocking call: it opens a socket and, for a
/// non-numeric bind host, resolves DNS, either of which can stall. It is therefore run on a dedicated
/// thread so [#start(ComponentContext)] returns a genuinely asynchronous stage that the runtime's
/// start deadline can bound. A synchronous bind here would run to completion on the caller's thread
/// before any stage existed, so a hanging bind would defeat the deadline and the rollback it triggers.
public final class ReceiverComponentAdapter implements Receiver, NetworkBound {

    /// Runs the blocking transport bind off the caller's thread, one virtual thread per start, so the
    /// returned stage completes asynchronously and the runtime's start deadline can bound a stalled
    /// bind (a slow DNS resolution of the configured host, for example) instead of hanging the caller.
    private static final Executor START_EXECUTOR =
            task -> Thread.ofVirtual().name("otlp-receiver-start").start(task);

    private final ServerReceiver receiver;

    /// Wraps an already-configured [ServerReceiver].
    ///
    /// @param receiver the transport receiver, its routed consumers already attached
    public ReceiverComponentAdapter(ServerReceiver receiver) {
        this.receiver = receiver;
    }

    /// Builds a receiver adapter from a transport receiver `builder`: it applies the neutral
    /// `config`, points the receiver at the base context's metrics and clock, attributes it to `id`,
    /// routes the graph consumers onto the builder, and builds the [ServerReceiver].
    ///
    /// @param builder   the transport-specific receiver builder
    /// @param config    the neutral receiver config
    /// @param consumers the routed graph consumers, one per served signal
    /// @param base      the base component context (its clock stamps each consume deadline, its
    ///                  metrics feed the admission instruments)
    /// @param id        the receiver component identity
    /// @return the receiver adapter, not yet started
    public static ReceiverComponentAdapter create(
            AbstractReceiverBuilder<?> builder,
            OtlpReceiverConfig config,
            SignalConsumers consumers,
            ComponentContext base,
            ComponentId id) {
        builder.setConfig(config.toServerConfig());
        builder.telemetry(base.metrics());
        builder.componentId(id);
        builder.clock(base.clock());
        for (var signal : consumers.signals()) {
            builder.route(signal, payloadConsumer(consumers, signal));
        }
        return new ReceiverComponentAdapter(builder.build());
    }

    @SuppressWarnings("unchecked")
    private static SignalConsumer<TelemetryPayload> payloadConsumer(
            SignalConsumers consumers, SignalDescriptor<?> descriptor) {
        // The graph stores a SignalConsumer<TelemetryPayload> under each SignalDescriptor, while the
        // public descriptor constants are keyed by the model batch type; the cast reconciles the two,
        // exactly as the runtime does when it assembles the bundle.
        var payloadDescriptor = (SignalDescriptor<TelemetryPayload>) (SignalDescriptor<?>) descriptor;
        return consumers.consumer(payloadDescriptor);
    }

    @Override
    public CompletionStage<Void> start(ComponentContext context) {
        // Offload the blocking bind so a stalled bind does not run to completion on the caller's
        // thread before a stage exists; a bind failure surfaces as this future's failure.
        return CompletableFuture.runAsync(receiver::start, START_EXECUTOR);
    }

    @Override
    public CompletionStage<Void> shutdown(ShutdownContext context) {
        return receiver.shutdown(context.deadline());
    }

    @Override
    public int port() {
        return receiver.port();
    }

    @Override
    public String toString() {
        return "ReceiverComponentAdapter(" + receiver + ")";
    }
}
