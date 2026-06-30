/// Model-to-proto marshalling shared by gRPC and HTTP transports. Touches only protobuf
/// message types, never service stubs.
///
/// Internal module: its API uses generated proto types, which `otlp4j-proto` exports only
/// to this module and the two transports - so the surface stays usable only by bundled
/// transports despite the unqualified export.
module dev.nthings.otlp4j.codec {
    requires transitive dev.nthings.otlp4j.model;
    requires transitive dev.nthings.otlp4j.proto;
    requires com.google.protobuf;

    exports dev.nthings.otlp4j.codec;
}
