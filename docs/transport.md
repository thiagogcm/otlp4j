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

Profiles are a `v1development` signal, and a request can name its schema version in the `OTLP-Profiles-Development-Version` header or `otlp-profiles-development-version` gRPC metadata. The receiver reads version 1, which is also what a request without that metadata means. It refuses a repeated value, a malformed value, or any other version before decoding the request, with `400` over HTTP and `INVALID_ARGUMENT` over gRPC. The exporter sends version 1 and omits the optional metadata.

## Configure an exporter

`OtlpExporterConfig` holds the endpoint, optional HTTP path prefix, headers, TLS, compression, retry policy, timeout policy, request/response byte caps, and the graph exporter's queue and redelivery policy. Use the record's builder for graph factories or pass the built record to an owned exporter.

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

An endpoint URL's scheme selects plaintext or TLS. A URL sets the port: its own, else `80` for `http` and `443` for `https`, as the OTLP exporter specification requires. The builder's seed port (4317 or 4318) applies when no URL is set. For HTTP, the exporter appends the signal path to the prefix: this example sends traces to `/otlp/v1/traces`. The bundled receiver serves the paths in the table; a prefix is useful when a proxy exposes it under another path.

`addHeader(key, value)` configures static headers. `headers(supplier)` supplies headers per export, for example to obtain a current credential. Supplier values override matching static keys and are validated on resolution. An invalid supplied header makes the export a permanent rejection that is not retried. Outgoing headers do not provide a receiver-side authentication policy.

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
| Graph exporter queue | 1,000 entries per signal, 10 consumers, `FAIL` when full |
| Graph exporter redelivery | 5 seconds initial delay, `1.5` multiplier, `0.5` jitter, 30 seconds cap, 5 minutes in total |

The shared exporter record requires positive byte caps no larger than `Integer.MAX_VALUE`. To change size caps on an owned builder, create a configuration record and pass it through `.config(config)`.

### Queue and redelivery

A graph exporter acknowledges a payload once it is in the exporter's own queue, and its workers then export it. An exporter id has one queue per signal, and all of its signals share one wire client. A queue redelivers a payload the destination failed retryably, which carries data through an outage longer than the wire retry. `DeliveryQueuePolicy` sets the capacity, consumer count, and full-queue behavior; `RedeliveryPolicy` sets the backoff and the total time to keep trying. See [Exporter queues and redelivery](delivery.md#exporter-queues-and-redelivery) for the semantics.

```java
import dev.nthings.otlp4j.core.policy.BackoffPolicy;
import dev.nthings.otlp4j.core.policy.DeliveryQueuePolicy;
import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.core.policy.RedeliveryPolicy;
import dev.nthings.otlp4j.transport.OtlpExporterConfig;
import java.time.Duration;

var config = OtlpExporterConfig.builder(4317)
        .endpoint("backend.example.com", 4317)
        .queue(new DeliveryQueuePolicy(5_000, 10, OverflowPolicy.FAIL))
        .redelivery(new RedeliveryPolicy(
                new BackoffPolicy(Duration.ofSeconds(2), 1.5, 0.5, Duration.ofSeconds(20)),
                Duration.ofMinutes(10)))
        .build();
```

With `noQueue()` the graph exporter exports synchronously and nothing redelivers: a retryable failure reaches the upstream sender as `503` or `UNAVAILABLE`, and a permanent one as `500` or `INTERNAL`. The queue settings apply to an exporter registered in a gateway or pipeline. An owned `OtlpExporter` from `grpcBuilder()` or `httpBuilder()` has no queue: its caller gets the result of the bounded export and decides what to do with it.

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

Successful OTLP responses with partial rejection are terminal and are not retried. Retries run in two layers, one inside the other:

| Layer | Governs | Gives up |
| --- | --- | --- |
| Wire retry (`RetryPolicy`, `TimeoutPolicy`) | One `export` call: up to five attempts within the total timeout | When attempts or the total timeout run out, and `export` completes with the last outcome |
| Queue redelivery (`RedeliveryPolicy`) | One queued payload: repeats the whole `export` call | When a wait would pass the five-minute budget, on a permanent rejection, or at shutdown |

One redelivery attempt is one `export` call, so a payload reaches the wire up to five times per redelivery attempt by default. The redelivery wait is the larger of its jittered backoff and the retry-after hint the failed export carried. A permanent rejection and a partial success are never redelivered. Neither layer is persistent: a queued payload is lost when the process stops. An owned `OtlpExporter` has the wire retry alone, and final failure ends that export operation.

### Export outcomes

`export` completes with a `DeliveryResult` for every wire outcome. The last outcome is classified with the decisions the retry loop used:

| Wire outcome | Result |
| --- | --- |
| HTTP `429`, `502`, `503`, `504`, a retryable gRPC status, or a transport failure (refused, reset, closed connection, TLS failure) | Retryable, downstream, with the server's wait hint when hints are respected |
| Any other non-2xx HTTP status or gRPC status, `RESOURCE_EXHAUSTED` without `RetryInfo`, or a negative gRPC pushback | Permanent, downstream |
| Total deadline expires | Retryable, reason `TIMEOUT`, downstream |
| Request cannot be prepared (over `maxRequestBytes`, invalid supplied header, unmarshallable payload) | Permanent, not downstream |
| Exporter is stopping or stopped | Retryable, reason `SHUTDOWN` |
| HTTP `2xx` whose body is empty or whose content type is not `application/x-protobuf` | Accepted |
| HTTP `2xx` declared protobuf that does not parse, or a response over `maxResponseBytes` | Permanent, downstream |

A `2xx` that cannot be read is never retryable: the server accepted the data, so re-sending would duplicate it. Caller cancellation and an exporter that was never started complete exceptionally. A graph exporter with `noQueue()` relays these results to its upstream sender; see [receiver responses](delivery.md#receiver-responses).

When positive server hints are enabled, HTTP `Retry-After` accepts delta seconds or an HTTP date. For gRPC, `RetryInfo` takes precedence over positive `grpc-retry-pushback-ms`, then the policy backoff applies. A negative gRPC pushback stops retries even when respecting positive hints is disabled.

The [delivery guide](delivery.md#receiver-responses) maps receiver results to wire responses and explains duplication risks when retrying a path with several destinations.

## Environment configuration

Environment loading is opt-in through `OtlpExporterConfig.Builder.fromEnvironment()` or `OtlpExporter.Builder.fromEnvironment()`. Explicit builder values win regardless of call order. Recognized values are parsed at build time, and malformed applicable values fail even when an explicit value would take precedence.

| Variable under `OTEL_EXPORTER_OTLP_` | Effect |
| --- | --- |
| `ENDPOINT` | Host, optional port (else the scheme default, 80 or 443), HTTP prefix, and TLS scheme; an explicitly set builder port wins |
| `HEADERS` | Comma-separated `key=value` pairs; percent-decode values |
| `TIMEOUT` | Positive milliseconds for the per-attempt timeout; also the total unless set separately |
| `COMPRESSION` | `gzip` or `none` |
| `CERTIFICATE` | PEM server trust file |
| `CLIENT_CERTIFICATE`, `CLIENT_KEY` | Paired PEM client credentials |
| `INSECURE` | Plaintext when true and no endpoint dictates the scheme |
| `PROTOCOL` | Validate `grpc` or `http/protobuf`; does not select the transport |

Select the protocol through its factory or owned builder. Per-signal variables and receiver environment configuration are not implemented. Use separate exporter registrations for different signal endpoints.

## Exact payload sizing

`OtlpPayloadSizer.exact()` returns a `PayloadSizer` that weighs any payload at the exact serialized size of its OTLP export request. Pass it to a component that limits work in bytes when its payloads have no wire form of their own. The batch processor guide explains [when it is needed and what it costs](components.md#how-submissions-are-weighed).

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
