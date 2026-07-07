/// The consolidated OTLP transport on Armeria: `OtlpArmeriaReceiver` and `OtlpArmeriaExporter`
/// serving and speaking OTLP/gRPC and OTLP/HTTP from one Netty-backed runtime behind the transport
/// SPI, replacing the two separate legacy transports.
module dev.nthings.otlp4j.transport.otlp {
    requires dev.nthings.otlp4j.model;
    // The graph factories name the core component and signal contracts directly.
    requires dev.nthings.otlp4j.core;
    requires transitive dev.nthings.otlp4j.transport.common;
    requires dev.nthings.otlp4j.codec;
    requires dev.nthings.otlp4j.proto;
    // Request sizing for admission control (MessageLite.getSerializedSize) and proto request types.
    requires com.google.protobuf;
    // Status and Metadata for the gRPC status mapping.
    requires io.grpc;
    // Generated OTLP service stubs and StreamObserver.
    requires io.grpc.stub;
    // com.google.rpc.Status / RetryInfo for decoding and encoding the grpc-status-details-bin trailer.
    // This jar declares no Automatic-Module-Name, so its module name is filename-derived (verified with
    // jar --describe-module against the resolved proto-google-common-protos jar).
    requires proto.google.common.protos;
    // The consolidated transport runtime: one Armeria server and client for both protocols.
    requires com.linecorp.armeria;
    requires com.linecorp.armeria.grpc;
    // Netty SslContextBuilder for the client TLS customizer.
    requires io.netty.handler;
    requires org.slf4j;

    exports dev.nthings.otlp4j.transport.otlp;
}
