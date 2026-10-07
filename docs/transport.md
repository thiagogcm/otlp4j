# OTLP transport

Add `otlp4j-transport` for OTLP receivers and exporters. The bundled implementation uses Armeria for gRPC and HTTP; its public configuration uses otlp4j and JDK types.

## Select a protocol

| Use | gRPC | HTTP binary protobuf |
| --- | --- | --- |
| Graph receiver factory | `OtlpReceiverFactory.grpc()` | `OtlpReceiverFactory.http()` |
| Graph exporter factory | `OtlpExporterFactory.grpc()` | `OtlpExporterFactory.http()` |
| Owned receiver builder | `OtlpReceiver.grpcBuilder()` | `OtlpReceiver.httpBuilder()` |
| Owned exporter builder | `OtlpExporter.grpcBuilder()` | `OtlpExporter.httpBuilder()` |
| Default port | 4317 | 4318 |

Both protocols carry all four signals. HTTP uses `POST` and `application/x-protobuf` with these routes:

| Signal   | HTTP path                 |
| -------- | ------------------------- |
| Traces   | `/v1/traces`              |
| Metrics  | `/v1/metrics`             |
| Logs     | `/v1/logs`                |
| Profiles | `/v1development/profiles` |

The HTTP receiver tolerates a missing content-type header. It rejects JSON and other content types with `415`, unsupported methods with `405`, malformed protobuf with `400`, and oversized decoded requests with `413`.

## Configure an exporter

`OtlpExporterConfig` holds the endpoint, optional HTTP path prefix, headers, TLS, compression, retry policy, timeout policy, and request/response byte caps. Use the record's builder for graph factories or pass the built record to an owned exporter.

```java
import dev.nthings.otlp4j.transport.Compression;
import dev.nthings.otlp4j.transport.OtlpExporterConfig;
import java.time.Duration;

var config = OtlpExporterConfig.builder(4318)
        .endpoint("https://telemetry.example.com:4318/otlp")
        .compression(Compression.GZIP)
        .timeout(Duration.ofSeconds(3))
        .totalTimeout(Duration.ofSeconds(15))
        .maxRequestBytes(8L * 1024 * 1024)
        .build();
```

An endpoint URL's scheme selects plaintext or TLS. If the URL omits a port, the builder retains its configured port. For HTTP, the exporter appends the signal path to the prefix: this example sends traces to `/otlp/v1/traces`. The bundled receiver serves the paths in the table; a prefix is useful when a proxy exposes it under another path.

`addHeader(key, value)` configures static headers. `headers(supplier)` supplies headers per export, for example to obtain a current credential. Supplier values override matching static keys and are validated on resolution. Invalid supplied headers fail the export exceptionally and are not retried. Outgoing headers do not provide a receiver-side authentication policy.

### Exporter defaults

| Setting | Default |
| --- | --- |
| Host and path prefix | `localhost`, empty prefix |
| TLS and compression | Disabled, `NONE` |
| Static headers and header supplier | Empty, absent |
| Maximum serialized request | 64 MiB |
| Maximum response | 4 MiB |
| Per-attempt and total timeout | 10 seconds each |
| Maximum attempts, including the initial request | 5 |
| Backoff initial delay, multiplier, jitter, cap | 1 second, `1.5`, `0.5`, 5 seconds |
| Respect positive server retry hints | `true` |

The shared exporter record requires positive byte caps no larger than `Integer.MAX_VALUE`. To change size caps on an owned builder, create a configuration record and pass it through `.config(config)`.

## Receiver admission and size limits

`OtlpReceiverConfig` controls the bind interface, port, TLS, client-authentication mode, decoded-request limit, client-timeout cap, and optional in-flight admission caps.

| Setting | Default | Meaning |
| --- | --- | --- |
| `bindHost` | `localhost` | Listen on the local interface |
| `tls`, `clientAuth` | Disabled, `NONE` | Plaintext without client-certificate authentication |
| `maxInboundMessageSizeBytes` | 64 MiB | Maximum decoded export request |
| `maxClientTimeout` | 30 seconds | Longest deadline a gRPC client may request |
| `maxInFlightRequests` | `0` | Request-count admission cap disabled |
| `maxInFlightBytes` | `0` | Decoded-byte admission cap disabled |
| `throttleRetryAfter` | Absent | No server admission retry hint |

The byte-admission cap spans decoded work in flight. A single request is allowed when nothing else is in flight, even if its size exceeds that cap. The separate per-request limit still applies. Admission caps are not a bound on all network buffers or JVM memory.

For example, this receiver configuration limits concurrent work and advertises a wait when at capacity:

```java
import dev.nthings.otlp4j.transport.OtlpReceiverConfig;
import java.time.Duration;

var receiverConfig = OtlpReceiverConfig.builder(4317)
        .bindHost("localhost")
        .maxInFlightRequests(64)
        .maxInFlightBytes(32L * 1024 * 1024)
        .throttleRetryAfter(Duration.ofSeconds(1))
        .build();
```

A gRPC client chooses its own deadline through the `grpc-timeout` header, and omitting the header means no deadline. `maxClientTimeout` replaces a longer or absent deadline with the cap, so an untrusted client cannot hold a request open indefinitely, and honours a shorter deadline as sent. The consumer sees the bounded value through `ConsumeContext.deadline()`. OTLP/HTTP clients cannot choose a deadline, so the cap does not apply to them.

Gzip requests are decoded transparently and checked against the decoded-size limit. The exporter selects request compression; receiver response compression is not configurable.

## TLS and client authentication

| TLS value | Use |
| --- | --- |
| `Tls.disabled()` | Plaintext |
| `Tls.systemTrust()` | Client TLS using the JVM's trust store |
| `Tls.trust(path)` or `Tls.trust(bytes)` | Client TLS using a custom PEM trust anchor |
| `Tls.custom(cert, key, trust)` | Server credentials or client-certificate credentials, using PEM paths or byte arrays |

A server requires a certificate and private key. `Tls.systemTrust()` alone cannot configure a server. Client certificate and key material must be supplied together.

Receiver `ClientAuth` controls certificate authentication:

| Mode       | Handshake behavior                                         |
| ---------- | ---------------------------------------------------------- |
| `NONE`     | Do not request a client certificate                        |
| `OPTIONAL` | Verify a presented certificate; accept clients without one |
| `REQUIRE`  | Require and verify a client certificate                    |

`OPTIONAL` and `REQUIRE` need server-side trust material. Missing required TLS material fails startup. For file-backed required mutual TLS:

```java
import dev.nthings.otlp4j.transport.ClientAuth;
import dev.nthings.otlp4j.transport.OtlpReceiverConfig;
import dev.nthings.otlp4j.transport.Tls;
import java.nio.file.Path;

var secured = OtlpReceiverConfig.builder(4317)
        .tls(Tls.custom(Path.of("server-cert.pem"), Path.of("server-key.pem"),
                Path.of("client-ca.pem")))
        .clientAuth(ClientAuth.REQUIRE)
        .build();
```

This authenticates certificates at the transport layer. Application authorization remains the host application's responsibility.

## Retries and time budgets

`RetryPolicy` caps attempts and defines backoff. `TimeoutPolicy` caps each attempt and the total delivery time, including waits. A caller's `ConsumeContext` deadline can shorten the total budget. Set a larger total budget explicitly when several attempts should fit; the default total budget does not grant ten seconds to every attempt.

| Protocol | Retry decisions |
| --- | --- |
| HTTP | Retry `429`, `502`, `503`, and `504`, plus transport failures; `408` is not retried |
| gRPC | Retry `CANCELLED`, `DEADLINE_EXCEEDED`, `ABORTED`, `OUT_OF_RANGE`, `UNAVAILABLE`, and `DATA_LOSS`, plus transport failures |
| gRPC resource exhaustion | Retry `RESOURCE_EXHAUSTED` only with a valid positive `RetryInfo` delay |

Successful OTLP responses with partial rejection are terminal and are not retried. An exporter has no persistent sending queue; final failure ends that export operation.

When positive server hints are enabled, HTTP `Retry-After` accepts delta seconds or an HTTP date. For gRPC, `RetryInfo` takes precedence over positive `grpc-retry-pushback-ms`, then the policy backoff applies. A negative gRPC pushback stops retries even when respecting positive hints is disabled.

The [delivery guide](delivery.md#receiver-responses) maps receiver results to wire responses and explains duplication risks when retrying a path with several destinations.

## Environment configuration

Environment loading is opt-in through `OtlpExporterConfig.Builder.fromEnvironment()` or `OtlpExporter.Builder.fromEnvironment()`. Explicit builder values win regardless of call order. Recognized values are parsed at build time, and malformed applicable values fail even when an explicit value would take precedence.

| Variable under `OTEL_EXPORTER_OTLP_` | Effect |
| --- | --- |
| `ENDPOINT` | Host, optional port, HTTP prefix, and TLS scheme |
| `HEADERS` | Comma-separated `key=value` pairs; percent-decode values |
| `TIMEOUT` | Positive milliseconds for the per-attempt timeout; also the total unless set separately |
| `COMPRESSION` | `gzip` or `none` |
| `CERTIFICATE` | PEM server trust file |
| `CLIENT_CERTIFICATE`, `CLIENT_KEY` | Paired PEM client credentials |
| `INSECURE` | Plaintext when true and no endpoint dictates the scheme |
| `PROTOCOL` | Validate `grpc` or `http/protobuf`; does not select the transport |

Select the protocol through its factory or owned builder. Per-signal variables and receiver environment configuration are not implemented. Use separate exporter registrations for different signal endpoints.

## Owned transports

An owned exporter is useful for direct delivery without a graph. For an existing trace `batch`:

```java
import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

import dev.nthings.otlp4j.transport.OtlpExporter;

try (var exporter = OtlpExporter.grpcBuilder().endpoint("localhost", 4317).build()) {
    exporter.start();
    var result = exporter.export(TRACES, batch).toCompletableFuture().join();
    System.out.println(result);
}
```

Owned receivers register typed `route(signal, consumer)` callbacks before build. `OtlpReceiver.builder()` serves both protocols on one listener; protocol-specific builders serve one. Set `ephemeralPort()` for tests and read `port()` after startup. Every accepted signal needs a route.

`build()` does not start wire resources. Owned `start()` calls are idempotent after successful startup, and `shutdown(timeout)` preserves its first result. `close()` uses a ten-second budget. Receiver cleanup remains graceful if the caller's wait expires; there is no force-stop API. Graph-owned transports follow the [runtime lifecycle](delivery.md#lifecycle-and-deadlines).
