# Getting started

This guide installs otlp4j and runs a typed trace pipeline that prints the number of accepted spans. It uses an in-process terminal callback, so you do not need a collector, network port, or credentials.

## Install the snapshot

Use JDK 25. From the repository root, install the modules into your local Maven repository:

```sh
./mvnw -B install
```

In your application, set the compiler release to `25` and add these dependencies:

```xml
<properties>
  <maven.compiler.release>25</maven.compiler.release>
</properties>

<dependencies>
  <dependency>
    <groupId>dev.nthings.otlp4j</groupId>
    <artifactId>otlp4j-model</artifactId>
    <version>0.1.0-SNAPSHOT</version>
  </dependency>
  <dependency>
    <groupId>dev.nthings.otlp4j</groupId>
    <artifactId>otlp4j-service</artifactId>
    <version>0.1.0-SNAPSHOT</version>
  </dependency>
</dependencies>
```

The service dependency brings the core contracts. Add modules only for the capabilities you use:

| Capability                          | Additional artifact    |
| ----------------------------------- | ---------------------- |
| OTLP network receivers or exporters | `otlp4j-transport`     |
| Built-in processors and connectors  | `otlp4j-components`    |
| OpenTelemetry Java SDK adapters     | `otlp4j-opentelemetry` |

All artifacts use the group and version shown above. A JPMS application also declares the modules it reads in `module-info.java`; see [module boundaries](architecture.md#modules).

## Run a first pipeline

Save this class as `FirstPipeline.java` in your application's sources and run its `main` method:

```java
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.model.Attributes;
import dev.nthings.otlp4j.model.InstrumentationScope;
import dev.nthings.otlp4j.model.Resource;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.service.Pipeline;
import java.util.List;

public class FirstPipeline {
    public static void main(String[] args) {
        var span = Span.builder()
                .traceId("0102030405060708090a0b0c0d0e0f10")
                .spanId("0102030405060708")
                .name("GET /checkout")
                .kind(Span.Kind.SERVER)
                .startEpochNanos(1_000L)
                .endEpochNanos(2_000L)
                .build();
        var batch = TracesData.of(
                Resource.of(Attributes.builder().put("service.name", "checkout").build()),
                InstrumentationScope.of("first-pipeline", "1.0"),
                List.of(span));

        var print = ExporterFactory.of("print", (context, payload, provenance) -> {
            System.out.println("Received " + payload.itemCount() + " span(s)");
            return DeliveryResult.acceptedStage(provenance);
        });

        try (var traces = Pipeline.builder(TRACES)
                .filter(data -> data.spanCount() > 0)
                .to(print)
                .build()) {
            traces.start();
            var result = traces.submit(batch).toCompletableFuture().join();
            if (!(result instanceof DeliveryResult.Accepted)) {
                throw new IllegalStateException("Submission was not fully accepted: " + result);
            }
        }
    }
}
```

The output is `Received 1 span(s)`. The pipeline has no buffer, so the submission completes after the print callback returns its acceptance. Try-with-resources shuts down the owned runtime.

## Continue with your use case

- [Typed pipelines](pipelines.md) shows OTLP export, transformations, and explicit buffering.
- [Gateway graphs](gateways.md) shows network input and multiple signal paths.
- [SDK integration](sdk-integration.md) connects existing instrumentation.
- [Sample scenarios](../otlp4j-samples/README.md) exercises two real OTLP hops without an external collector.

Before adding buffering or retries, read [Delivery and lifecycle](delivery.md) so your application handles both submission results and later delivery losses.
