package dev.nthings.otlp4j.transport;

import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Receiver;
import dev.nthings.otlp4j.core.component.ReceiverFactory;
import dev.nthings.otlp4j.core.consumer.SignalConsumers;
import dev.nthings.otlp4j.transport.internal.Protocol;
import dev.nthings.otlp4j.transport.internal.ReceiverComponentAdapter;

/// The graph [ReceiverFactory] for the OTLP receiver, protocol-parameterized through [#grpc()] and
/// [#http()]. Each factory creates one receiver serving every routed signal from one port by wiring
/// the matching [OtlpReceiver] builder onto the graph consumers through a [ReceiverComponentAdapter],
/// which exposes the bound port through the gateway runtime after startup. The protocol derives the
/// component [#type()] (`otlp` for gRPC, `otlphttp` for HTTP) and the
/// [#defaultPort()] (`4317` / `4318`).
public final class OtlpReceiverFactory implements ReceiverFactory<OtlpReceiverConfig> {

    private final Protocol protocol;
    private final String type;
    private final int defaultPort;

    private OtlpReceiverFactory(Protocol protocol, String type, int defaultPort) {
        this.protocol = protocol;
        this.type = type;
        this.defaultPort = defaultPort;
    }

    /// The OTLP/gRPC receiver factory (`otlp`, default port `4317`).
    ///
    /// @return the gRPC receiver factory
    public static OtlpReceiverFactory grpc() {
        return new OtlpReceiverFactory(Protocol.GRPC, "otlp", Ports.GRPC);
    }

    /// The OTLP/HTTP receiver factory (`otlphttp`, default port `4318`).
    ///
    /// @return the HTTP receiver factory
    public static OtlpReceiverFactory http() {
        return new OtlpReceiverFactory(Protocol.HTTP, "otlphttp", Ports.HTTP);
    }

    /// The conventional port for this factory's protocol (`4317` for gRPC, `4318` for HTTP).
    ///
    /// @return the default bind port
    public int defaultPort() {
        return defaultPort;
    }

    @Override
    public ComponentKind kind() {
        return ComponentKind.RECEIVER;
    }

    @Override
    public OtlpReceiverConfig defaultConfig() {
        return OtlpReceiverConfig.defaults(defaultPort);
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public Receiver create(CreateContext ctx, OtlpReceiverConfig config, SignalConsumers consumers) {
        return ReceiverComponentAdapter.create(builder(), config, consumers, ctx.base(), ctx.id());
    }

    /// The [OtlpReceiver] builder serving this factory's protocol only.
    private OtlpReceiver.Builder builder() {
        return switch (protocol) {
            case GRPC -> OtlpReceiver.grpcBuilder();
            case HTTP -> OtlpReceiver.httpBuilder();
        };
    }
}
