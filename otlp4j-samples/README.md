# End-to-end sample

`OtlpE2eDemo` exercises the public API and the bundled OTLP/gRPC transport together. The sample builds its component graph with `Gateway.builder()` on `otlp4j-core` and `otlp4j-service`, wires the consolidated Armeria transport through `otlp4j-transport` (`OtlpReceiverFactory.grpc()` / `OtlpExporterFactory.grpc()` with the neutral `OtlpReceiverConfig` / `OtlpExporterConfig`), and adds the built-in batch processor and span-count connector from `otlp4j-components`; it never imports the generated proto or gRPC stub types, which stay encapsulated in `otlp4j-proto`.

## Scenario

```mermaid
sequenceDiagram
    participant Client
    participant Gateway
    participant Pipeline
    participant Backend

    Client->>Gateway: Export 5 spans
    Gateway->>Pipeline: TracesData
    Pipeline->>Pipeline: Enrich resource and keep 3 SERVER spans
    par Forward traces
        Pipeline->>Backend: Export 3 spans
    and Derive metric
        Pipeline->>Backend: Export span count = 3
    end
    Pipeline-->>Client: Accepted
```

Both receivers use ephemeral ports. The test asserts that the backend receives three spans, `deployment.environment=demo`, and an `otlp4j.connector.span.count` metric with value `3`.

Run it from the repository root:

```sh
./mvnw -B -pl otlp4j-samples -am test \
  -Dtest=OtlpE2eDemoTest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

No external collector is required.

## Optional packaging

```sh
./mvnw -B -pl otlp4j-samples -am package -Pnative
./mvnw -B -pl otlp4j-samples -am package -Pjlink
```

The `native` profile requires a GraalVM JDK. The `jlink` profile creates the pure API/sample runtime image; the gRPC/Netty stack (automatic modules) remains outside the linked image.
