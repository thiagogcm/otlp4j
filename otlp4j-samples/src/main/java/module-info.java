/// Runnable end-to-end demonstrations compiled against the public API only.
///
/// The transport is supplied at runtime through the API's service-provider interface.
module dev.nthings.otlp4j.samples {
    requires dev.nthings.otlp4j.api;

    // The logback backend is an automatic module bound at runtime via ServiceLoader,
    // so only the slf4j facade is required here.
    requires org.slf4j;
}
