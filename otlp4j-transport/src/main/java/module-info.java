/// The consolidated OTLP transport on Armeria: [dev.nthings.otlp4j.transport.OtlpExporter] and
/// [dev.nthings.otlp4j.transport.OtlpReceiver] (standalone entry points), and
/// [dev.nthings.otlp4j.transport.OtlpExporterFactory] / [dev.nthings.otlp4j.transport.OtlpReceiverFactory]
/// (the graph component factories). One Armeria plus Netty runtime serves and speaks both OTLP/gRPC and
/// OTLP/HTTP. The Armeria, Netty, and proto types stay confined to the unexported `internal` package.
@SuppressWarnings("module")
module dev.nthings.otlp4j.transport {
    requires dev.nthings.otlp4j.model;
    // The codec payload facade (Payloads), the signal codecs, and the delivery-result status mapping.
    requires dev.nthings.otlp4j.codec;
    // The config records expose the neutral core policies, and the exporter runtime, receiver runtime,
    // and graph adapters name core component, consumer, result, and signal types directly, so read core
    // transitively.
    requires transitive dev.nthings.otlp4j.core;
    // Generated OTLP proto message and service stub types the wire client and server marshal.
    requires dev.nthings.otlp4j.proto;
    // Request sizing for admission control (MessageLite.getSerializedSize) and proto request types.
    requires com.google.protobuf;
    // Status and Metadata for the gRPC status mapping.
    requires io.grpc;
    // Generated OTLP service stubs and StreamObserver.
    requires io.grpc.stub;
    // com.google.rpc.Status / RetryInfo for decoding and encoding the grpc-status-details-bin trailer.
    // This jar declares no Automatic-Module-Name, so its module name is filename-derived.
    requires proto.google.common.protos;
    // The consolidated transport runtime: one Armeria server and client for both protocols.
    requires com.linecorp.armeria;
    requires com.linecorp.armeria.grpc;
    // Netty SslContextBuilder / ClientAuth for the TLS mapping.
    requires io.netty.handler;
    requires org.slf4j;

    exports dev.nthings.otlp4j.transport;
}
