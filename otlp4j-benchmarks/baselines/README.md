# Performance baselines

This directory keeps dated JMH JSON captures for the two paths that matter to the library:

- `ReceiveForwardExportBenchmark` sends a traces batch through a real OTLP receiver, graph, and exporter, with both hops using the selected gRPC or HTTP protocol.
- `CodecMarshallingBenchmark` measures protobuf parse/decode/encode/serialize and same-protocol parse/serialize without sockets.

The JSON files are historical measurements, not release-performance guarantees. Compare captures made with the same JDK, machine, parameters, and benchmark version.

## Produce a baseline

Run on an otherwise quiet machine:

```sh
just bench-baseline
```

The recipe builds the shaded benchmark jar and writes a dated JSON result:

```sh
./mvnw -B -Pbench -pl otlp4j-benchmarks -am package -DskipTests
java -jar otlp4j-benchmarks/target/benchmarks.jar -prof gc \
    -rf json -rff otlp4j-benchmarks/baselines/baseline-<date>.json
```

The `bench` profile keeps benchmarks out of the default reactor. The harness is non-modular and the shade configuration merges gRPC service-provider files so the real host/port path remains usable.

## Parameters

Both benchmark classes use one fork, three one-second warmup iterations, and five one-second measurement iterations. The end-to-end benchmark varies:

| Parameter  | Values           |
| ---------- | ---------------- |
| `protocol` | `GRPC`, `HTTP`   |
| `size`     | `small`, `large` |

The codec benchmark varies `signal` (`TRACES`, `METRICS`, `LOGS`) and the same size labels. `small` contains one item; `large` contains 500 items.

Each run uses JMH throughput and average-time modes with the GC profiler. For comparisons, use `primaryMetric.score` for throughput and `secondaryMetrics."gc.alloc.rate.norm".score` for bytes per operation. Higher throughput and lower allocation are better.

## Recorded files

| File | Date | JDK | Environment |
| --- | --- | --- | --- |
| `baseline-2026-07-06.json` | 2026-07-06 | Temurin 25.0.3+9 | Intel Core Ultra 9 185H, 22 logical cores, 16 GB RAM, Linux 6.18 WSL2 |
| `phase2-2026-07-07.json` | 2026-07-07 | Temurin 25.0.3+9 | Intel Core Ultra 9 185H, 22 logical cores, 16 GB RAM, Linux 6.18 WSL2 |
| `phase4-2026-07-07.json` | 2026-07-07 | Temurin 25.0.3+9 | Intel Core Ultra 9 185H, 22 logical cores, 16 GB RAM, Linux 6.18 WSL2 |

The older filenames remain to preserve measurement history. `phase4-2026-07-07.json` is the capture using the consolidated Armeria transport; its benchmark parameters predate the current API cleanup. Keep all existing JSON files when adding a new capture.
