/// Runnable end-to-end demonstrations of otlp4j.
///
/// Boots the gateway and the backend as service-graph runtimes from the service module, wiring the
/// core component contracts, the neutral transport configs, the concrete OTLP/gRPC factories from the
/// consolidated Armeria transport, and the built-in batch processor and span-count connector from
/// otlp4j-components; it never touches the generated proto code.
module dev.nthings.otlp4j.samples {
    // The core component, signal, policy, and result contracts the demo and its seed factories name.
    requires dev.nthings.otlp4j.core;
    // The service graph runtime the demo boots the gateway and the backend through.
    requires dev.nthings.otlp4j.service;
    // The neutral OTLP receiver and exporter configs the demo hands to the transport factories.
    // The concrete OTLP/gRPC receiver and exporter factories and the client exporter the demo wires,
    // from the consolidated Armeria transport.
    requires dev.nthings.otlp4j.transport;
    // The built-in batch processor and span-count connector the demo gateway pipeline wires.
    requires dev.nthings.otlp4j.components;
    // The domain batch records the demo builds and inspects.
    requires dev.nthings.otlp4j.model;
    // The proto-free codec payload facade (Payloads) the seed processors emit through.
    requires dev.nthings.otlp4j.codec;

    // The logback backend is an automatic module bound at runtime via ServiceLoader,
    // so only the slf4j facade is required here.
    requires org.slf4j;
}
