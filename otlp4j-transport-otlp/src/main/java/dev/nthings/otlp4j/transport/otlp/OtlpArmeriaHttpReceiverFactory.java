package dev.nthings.otlp4j.transport.otlp;

import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Receiver;
import dev.nthings.otlp4j.core.component.ReceiverFactory;
import dev.nthings.otlp4j.core.consumer.SignalConsumers;
import dev.nthings.otlp4j.transport.common.OtlpReceiverConfig;
import dev.nthings.otlp4j.transport.common.ReceiverComponentAdapter;

/// The graph [ReceiverFactory] for the Armeria OTLP/HTTP receiver. It creates one receiver serving
/// every routed signal from one port by wiring the [OtlpArmeriaReceiver] HTTP builder onto the graph
/// consumers through a [ReceiverComponentAdapter], which also exposes the bound port through
/// [dev.nthings.otlp4j.core.component.NetworkBound]. Mirror of the legacy OTLP/HTTP receiver factory.
public final class OtlpArmeriaHttpReceiverFactory implements ReceiverFactory<OtlpReceiverConfig> {

    /// The conventional OTLP/HTTP port (4318).
    public static final int DEFAULT_PORT = 4318;

    /// The component type name for the OTLP/HTTP receiver.
    public static final String TYPE = "otlphttp";

    @Override
    public ComponentKind kind() {
        return ComponentKind.RECEIVER;
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
                OtlpArmeriaReceiver.httpBuilder(), config, consumers, ctx.base(), ctx.id());
    }
}
