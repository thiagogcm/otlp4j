# Performance measurements

Use the JMH harness to compare marshalling cost and a real receive-forward-export path. A result describes the measured workload and environment; it is not a release performance guarantee.

| Benchmark | Measured work |
| --- | --- |
| `ReceiveForwardExportBenchmark.receiveForwardExport` | A trace batch crosses a client, gateway receiver/exporter, and backend receiver over two network hops |
| `CodecMarshallingBenchmark.parseDecodeEncodeSerialize` | Parse protobuf, construct the domain batch, re-encode, and serialize |
| `CodecMarshallingBenchmark.parseSerialize` | Parse and serialize protobuf without constructing a domain batch |

## Run measurements

From the repository root, run on a quiet machine:

```sh
just bench
```

The recipe builds the optional benchmark module and runs the harness with the GC profiler. Without `just`:

```sh
./mvnw -B -Pbench -pl otlp4j-benchmarks -am package -DskipTests
java -jar otlp4j-benchmarks/target/benchmarks.jar -prof gc
```

The `bench` profile keeps JMH out of the default reactor. The harness uses a shaded, non-modular jar with merged service-provider entries for the wire transport.

## Record a comparable result

`just bench-baseline` writes a dated JSON capture into this directory. Before recording, check whether the destination exists; choose a distinct label to preserve an existing result:

```sh
just bench-baseline comparison-local
```

For a temporary JSON result without `just`, use the built jar:

```sh
java -jar otlp4j-benchmarks/target/benchmarks.jar -prof gc \
  -rf json -rff /tmp/otlp4j-jmh.json
```

## Workload parameters

| Benchmark              | Parameter  | Values                      |
| ---------------------- | ---------- | --------------------------- |
| Receive-forward-export | `protocol` | `GRPC`, `HTTP`              |
| Receive-forward-export | `size`     | `small`, `large`            |
| Codec methods          | `signal`   | `TRACES`, `METRICS`, `LOGS` |
| Codec methods          | `size`     | `small`, `large`            |

`small` contains one item; `large` contains 500. The codec harness excludes profiles. The classes use one fork, three one-second warmup iterations, and five one-second measurement iterations, with throughput and average-time modes.

## Interpret results

| Quantity | JSON field | Preferred direction |
| --- | --- | --- |
| Throughput | `primaryMetric.score` in a throughput entry | Higher |
| Average time | `primaryMetric.score` in an average-time entry | Lower |
| Allocation per operation | `secondaryMetrics["gc.alloc.rate.norm"].score` | Lower |

Read the recorded units. The network benchmark's time unit is milliseconds; the codec benchmark's is microseconds. Do not compare scores across different modes or units without conversion.

Keep the JDK, machine, JVM options, parameters, and benchmark implementation consistent when comparing captures. Record environment details with each new measurement. Inspect the parameters in each JSON file before using it as a comparison input.

The measurements cover specific paths. They do not establish queue behavior under overload, production backend latency, or the cost of arbitrary application transforms. For the implementation boundaries behind the paths, see [Architecture](../../docs/architecture.md).
