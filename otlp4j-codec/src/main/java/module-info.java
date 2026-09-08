/// Model-to-proto marshalling shared by gRPC and HTTP transports. Touches only protobuf
/// message types, never service stubs.
///
/// Internal module: generated proto and wire-adapter types are qualified-exported only to the bundled
/// components and transport modules. Applications use core's typed payload API.
@SuppressWarnings("module")
module dev.nthings.otlp4j.codec {
    requires transitive dev.nthings.otlp4j.model;
    requires transitive dev.nthings.otlp4j.proto;
    // The DeliveryResult type the response mapping is built around.
    requires dev.nthings.otlp4j.core;
    requires com.google.protobuf;

    exports dev.nthings.otlp4j.codec to dev.nthings.otlp4j.components, dev.nthings.otlp4j.transport;
}
