package dev.nthings.otlp4j.spi;

/// The wire protocol an OTLP transport speaks.
///
/// A transport provider declares the protocol it implements via [TransportProvider#protocol()], and
/// the high-level exporter/receiver select a provider by protocol rather than by `ServiceLoader`
/// ordering. This is what lets the OTLP/gRPC and OTLP/HTTP transports coexist on one module/class
/// path: the user chooses by instantiating `OtlpGrpcExporter`/`OtlpGrpcReceiver` or
/// `OtlpHttpExporter`/`OtlpHttpReceiver`.
public enum Protocol {

    /// OTLP over gRPC — binary protobuf framed by gRPC. Conventional default port 4317.
    GRPC,

    /// OTLP over HTTP with binary-protobuf request bodies (`application/x-protobuf`). Conventional
    /// default port 4318.
    HTTP_PROTOBUF
}
