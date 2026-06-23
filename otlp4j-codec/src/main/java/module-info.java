/// Model⇄proto marshalling shared by the gRPC and HTTP transports. gRPC-free in code: it touches
/// only protobuf message types, never the service stubs.
module dev.nthings.otlp4j.codec {
    requires transitive dev.nthings.otlp4j.model;
    requires dev.nthings.otlp4j.proto;
    requires com.google.protobuf;

    // Unqualified, not `exports … to` the transports: this compiles before they exist in the reactor,
    // and a qualified forward reference trips -Werror. Internal, javadoc-skipped artifact.
    exports dev.nthings.otlp4j.codec;
}
