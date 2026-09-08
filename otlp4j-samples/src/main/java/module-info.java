/// Runnable in-process and end-to-end demonstrations of otlp4j.
///
/// The primary embedded demo uses the typed pipeline API; the network demo boots gateway and backend
/// service-graph runtimes, wiring the core contracts, transport factories, and built-in components. No
/// sample touches generated protobuf code.
module dev.nthings.otlp4j.samples {
    // The core component, signal, policy, and result contracts the demo and its seed factories name.
    requires dev.nthings.otlp4j.core;
    // The service graph runtime the demo boots the gateway and the backend through.
    requires dev.nthings.otlp4j.service;
    // The owned OTLP receiver and exporter runtimes, configs, and graph factories from the
    // consolidated Armeria transport.
    requires dev.nthings.otlp4j.transport;
    // The built-in batch processor and span-count connector the demo gateway pipeline wires.
    requires dev.nthings.otlp4j.components;
    // The domain batch records the demo builds and inspects.
    requires dev.nthings.otlp4j.model;
    // The logback backend is an automatic module bound at runtime via ServiceLoader,
    // so only the slf4j facade is required here.
    requires org.slf4j;
    // The JPMS consumer test invokes the JDK compiler; this optional requirement is not part of the
    // runnable samples' runtime dependencies.
    requires static java.compiler;
}
