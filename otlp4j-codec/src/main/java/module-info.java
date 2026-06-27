/// Model⇄proto marshalling shared by the gRPC and HTTP transports. gRPC-free in code: it touches
/// only protobuf message types, never the service stubs.
///
/// Internal module: the `codec` package is a qualified export to the two bundled transports only, so
/// it never widens the supported public surface. The export targets are compiled after this module
/// in the reactor, so javac cannot yet see them and emits a `module not found` lint warning; the
/// module-level `@SuppressWarnings("module")` scopes the opt-out to this declaration alone.
@SuppressWarnings("module")
module dev.nthings.otlp4j.codec {
    requires transitive dev.nthings.otlp4j.model;
    requires transitive dev.nthings.otlp4j.proto;
    requires com.google.protobuf;

    exports dev.nthings.otlp4j.codec to
            dev.nthings.otlp4j.transport.grpc,
            dev.nthings.otlp4j.transport.http;
}
