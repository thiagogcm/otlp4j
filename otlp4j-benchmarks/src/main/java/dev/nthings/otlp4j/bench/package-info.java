/// JMH benchmark harness capturing the Phase 0 receive-forward-export throughput and allocation
/// baseline.
///
/// The package holds three pieces: [dev.nthings.otlp4j.bench.BenchmarkPayloads] builds the small and
/// large domain batches once, [dev.nthings.otlp4j.bench.ReceiveForwardExportBenchmark] measures the
/// end-to-end receive-forward-export path over real OTLP/gRPC and OTLP/HTTP transports, and
/// [dev.nthings.otlp4j.bench.CodecMarshallingBenchmark] isolates the pure parse, decode, encode, and
/// serialize marshalling cost the Phase 2 payload redesign targets. The module is built only under
/// the Maven `bench` profile and is never compiled by the default reactor.
package dev.nthings.otlp4j.bench;
