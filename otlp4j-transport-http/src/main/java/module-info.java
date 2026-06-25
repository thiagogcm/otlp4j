/// The OTLP/HTTP transport: `OtlpHttpExporter` and `OtlpHttpReceiver`, built on `java.net.http` and
/// `jdk.httpserver`. No gRPC or Netty dependency.
module dev.nthings.otlp4j.transport.http {
    requires dev.nthings.otlp4j.model;
    requires transitive dev.nthings.otlp4j.api;
    requires dev.nthings.otlp4j.codec;
    requires dev.nthings.otlp4j.proto;
    requires com.google.protobuf;
    requires java.net.http;
    requires jdk.httpserver;
    requires org.slf4j;

    exports dev.nthings.otlp4j.transport.http;
}
