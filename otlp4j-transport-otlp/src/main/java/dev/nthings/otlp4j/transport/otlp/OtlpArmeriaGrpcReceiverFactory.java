package dev.nthings.otlp4j.transport.otlp;

import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Receiver;
import dev.nthings.otlp4j.core.component.ReceiverFactory;
import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.core.consumer.SignalConsumers;
import dev.nthings.otlp4j.transport.common.OtlpReceiverConfig;
import dev.nthings.otlp4j.transport.common.ReceiverComponentAdapter;

/// The graph [ReceiverFactory] for the Armeria OTLP/gRPC receiver. It creates one receiver serving
/// every routed signal from one port by wiring the [OtlpArmeriaReceiver] gRPC builder onto the graph
/// consumers through a [ReceiverComponentAdapter], which also exposes the bound port through
/// [dev.nthings.otlp4j.core.component.NetworkBound]. Mirror of the legacy OTLP/gRPC receiver factory.
public final class OtlpArmeriaGrpcReceiverFactory implements ReceiverFactory<OtlpReceiverConfig> {

    /// The conventional OTLP/gRPC port (4317).
    public static final int DEFAULT_PORT = 4317;

    /// The component type name for the OTLP/gRPC receiver.
    public static final String TYPE = "otlp";

    @Override
    public ComponentKind kind() {
        return ComponentKind.RECEIVER;
    }

    @Override
    public StabilityLevel stability() {
        return StabilityLevel.BETA;
    }

    @Override
    public OtlpReceiverConfig defaultConfig() {
        return OtlpReceiverConfig.defaults(DEFAULT_PORT);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Receiver create(CreateContext ctx, OtlpReceiverConfig config, SignalConsumers consumers) {
        return ReceiverComponentAdapter.create(
                OtlpArmeriaReceiver.grpcBuilder(), config, consumers, ctx.base(), ctx.id());
    }
}
