/// Runnable end-to-end demonstrations of otlp4j.
///
/// Uses the pipeline DSL from the API and the concrete OTLP/gRPC entry points from the gRPC
/// transport module; it never touches the generated proto code.
module dev.nthings.otlp4j.samples {
    requires dev.nthings.otlp4j.api;
    requires dev.nthings.otlp4j.transport.grpc;

    // The logback backend is an automatic module bound at runtime via ServiceLoader,
    // so only the slf4j facade is required here.
    requires org.slf4j;
}
