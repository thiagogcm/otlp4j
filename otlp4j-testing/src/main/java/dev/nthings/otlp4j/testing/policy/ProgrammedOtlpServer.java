package dev.nthings.otlp4j.testing.policy;

import dev.nthings.otlp4j.testing.conformance.Protocol;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/// A small reusable fake OTLP collector that replays a [ProgrammedResponse] script one element per
/// received attempt and records the wall-clock arrival of each attempt, so a policy case can read the
/// attempt count and the gaps between attempts. One implementation drives OTLP/HTTP over the JDK
/// `HttpServer`, the other OTLP/gRPC over grpc-netty; both share the recording, over-script tolerance,
/// and attempt accounting on this base. A fresh server is booted on an ephemeral loopback port per
/// export and closed in the same call, so no state leaks between cases.
///
/// Over-script tolerance is part of the contract. A retry may already be scheduled when the total
/// deadline expires, so a straggler attempt can arrive after the script is exhausted. When that
/// happens the server still records the arrival (so the attempt count stays
/// observable) and answers with a default retryable [ProgrammedResponse.Unavailable], never hanging and
/// never throwing. Because each server owns a private port and is closed during the same export's
/// teardown, a late straggler can only reach its own already-draining server, never a later case's.
public abstract sealed class ProgrammedOtlpServer implements AutoCloseable
        permits HttpProgrammedServer, GrpcProgrammedServer {

    private final List<ProgrammedResponse> script;
    private final List<Long> arrivalNanos = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger cursor = new AtomicInteger();

    /// Seeds the base with the per-attempt script.
    ///
    /// @param script the per-attempt server behaviours, replayed in order
    protected ProgrammedOtlpServer(List<ProgrammedResponse> script) {
        this.script = List.copyOf(script);
    }

    /// Boots a fresh server for `protocol` on an ephemeral loopback port with `script`.
    ///
    /// @param protocol the transport to boot
    /// @param script   the per-attempt server behaviours
    /// @return the started server, which the caller closes
    public static ProgrammedOtlpServer start(Protocol protocol, List<ProgrammedResponse> script) {
        return switch (protocol) {
            case HTTP -> HttpProgrammedServer.started(script);
            case GRPC -> GrpcProgrammedServer.started(script);
        };
    }

    /// Records the arrival of the current attempt and returns the behaviour to answer it with. Beyond
    /// the scripted length this records the arrival and returns a default retryable
    /// [ProgrammedResponse.Unavailable], so a straggler cannot corrupt the count or stall the server.
    ///
    /// @return the behaviour to answer this attempt with
    protected final ProgrammedResponse recordAndNext() {
        long now = System.nanoTime();
        int index = cursor.getAndIncrement();
        arrivalNanos.add(now);
        return index < script.size() ? script.get(index) : new ProgrammedResponse.Unavailable();
    }

    /// The number of attempts that reached the server.
    ///
    /// @return the attempt count
    public final int attempts() {
        synchronized (arrivalNanos) {
            return arrivalNanos.size();
        }
    }

    /// The gaps between consecutive attempt arrivals, one fewer than [#attempts()].
    ///
    /// @return the inter-attempt gaps
    public final List<Duration> interAttemptGaps() {
        synchronized (arrivalNanos) {
            var gaps = new ArrayList<Duration>(Math.max(0, arrivalNanos.size() - 1));
            for (int i = 1; i < arrivalNanos.size(); i++) {
                gaps.add(Duration.ofNanos(arrivalNanos.get(i) - arrivalNanos.get(i - 1)));
            }
            return List.copyOf(gaps);
        }
    }

    /// The bound ephemeral port.
    ///
    /// @return the port
    public abstract int port();

    @Override
    public abstract void close();
}
