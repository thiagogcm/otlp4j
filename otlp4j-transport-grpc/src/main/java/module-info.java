/// The OTLP/gRPC transport: [OtlpGrpcExporter] and [OtlpGrpcReceiver], built on
/// gRPC + Netty. The only module that pulls in `grpc-netty`, so an HTTP-only
/// deployment never carries Netty.
module dev.nthings.otlp4j.transport.grpc {
    requires dev.nthings.otlp4j.model;
    requires transitive dev.nthings.otlp4j.api;
    requires transitive dev.nthings.otlp4j.transport.spi;
    requires dev.nthings.otlp4j.codec;
    requires dev.nthings.otlp4j.proto;
    requires io.grpc;
    requires io.grpc.stub;
    requires io.grpc.netty;
    requires org.slf4j;

    exports dev.nthings.otlp4j.transport.grpc;
}
