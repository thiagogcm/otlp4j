/// Model⇄proto marshalling shared by the gRPC and HTTP transports. gRPC-free in code: it touches
/// only protobuf message types, never the service stubs.
///
/// Internal module: its API is typed in the generated proto messages, which `otlp4j-proto` exports
/// only to this module and the two transports — so the surface stays usable only by the bundled
/// transports despite the unqualified export.
module dev.nthings.otlp4j.codec {
    requires transitive dev.nthings.otlp4j.model;
    requires transitive dev.nthings.otlp4j.proto;
    requires com.google.protobuf;

    exports dev.nthings.otlp4j.codec;
}
