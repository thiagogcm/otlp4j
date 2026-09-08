/// JMH benchmark harness capturing receive-forward-export throughput and allocation baselines.
///
/// The package holds three pieces: [dev.nthings.otlp4j.bench.BenchmarkPayloads] builds the small and
/// large domain batches once, [dev.nthings.otlp4j.bench.ReceiveForwardExportBenchmark] measures the
/// end-to-end receive-forward-export path over real OTLP/gRPC and OTLP/HTTP transports, and
/// [dev.nthings.otlp4j.bench.CodecMarshallingBenchmark] isolates pure parse, decode, encode, and
/// serialize marshalling cost. The module is built only under the Maven `bench` profile and is never
/// compiled by the default reactor.
package dev.nthings.otlp4j.bench;
