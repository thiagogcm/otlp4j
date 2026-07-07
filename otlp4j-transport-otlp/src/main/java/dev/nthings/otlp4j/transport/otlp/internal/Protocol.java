package dev.nthings.otlp4j.transport.otlp.internal;

/// The OTLP wire protocols the consolidated Armeria transport can serve or speak. One Armeria server
/// can serve both on one port; the receiver and exporter factories each select a single protocol to
/// mirror the two legacy transports' ports (`4317` gRPC, `4318` HTTP).
public enum Protocol {

    /// OTLP over gRPC.
    GRPC,

    /// OTLP over HTTP with binary protobuf bodies.
    HTTP
}
