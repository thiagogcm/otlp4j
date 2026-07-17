package dev.nthings.otlp4j.transport.internal;

import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import dev.nthings.otlp4j.codec.SignalCodec;
import dev.nthings.otlp4j.transport.OtlpReceiverConfig;
import dev.nthings.otlp4j.transport.Tls;
import dev.nthings.otlp4j.core.telemetry.GatewayMetrics;
import dev.nthings.otlp4j.transport.internal.AdmissionControl;
import dev.nthings.otlp4j.transport.OtlpReceiver.ClientAuth;
import java.io.IOException;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// The consolidated OTLP implementation of the wire server contract, built on one Armeria [Server].
///
/// One server serves OTLP/gRPC, OTLP/HTTP, or both from a single port, chosen by the protocol set.
/// The gRPC side registers the four OTLP collector services through `GrpcService`; the HTTP side
/// registers the four per-signal `/v1/...` routes as POST-only services. Both share one
/// [AdmissionControl] so the in-flight caps and the throttle hint behave identically to the legacy
/// servers. [Tls] selects the server credentials: [Tls.Disabled] binds plaintext, [Tls.Custom] and
/// [Tls.Inline] supply the certificate and key, and [Tls.SystemTrust] and [Tls.SslContext] are
/// rejected as a server has no certificate of its own. The oversize path differs by protocol: gRPC
/// enforces the decoded-message cap through the `GrpcService` builder (`RESOURCE_EXHAUSTED`), while
/// HTTP enforces it in the request mapping (`413`).
public final class ArmeriaOtlpServer {

    private static final Logger log = LoggerFactory.getLogger(ArmeriaOtlpServer.class);

    /// The build-time graceful-shutdown drain window. The SPI drain deadline supplied to
    /// [#shutdown(Duration)] bounds the returned stage; this window bounds how long Armeria itself
    /// waits for in-flight requests to complete before it aborts them.
    private static final Duration DRAIN_WINDOW = Duration.ofSeconds(30);

    /// The graceful-shutdown quiet period. A non-zero quiet period is what makes Armeria install its
    /// real graceful-shutdown tracker: `stop()` then waits for in-flight requests to complete (and for
    /// this idle window to pass with no new request) before it tears connections down, up to
    /// [#DRAIN_WINDOW]. A zero quiet period disables the tracker and aborts in-flight requests at once,
    /// which would drop telemetry on every rolling restart, so the quiet period is kept non-zero.
    private static final Duration QUIET_PERIOD = Duration.ofSeconds(1);

    /// The prompt bound applied by [#shutdownNow()] so the returned stage settles quickly even while
    /// Armeria finishes tearing the server down in the background.
    private static final Duration FORCEFUL_BOUND = Duration.ofSeconds(5);

    private record HttpRoute(SignalCodec<?, ?, ?> codec, String path) {}

    private static final List<HttpRoute> HTTP_ROUTES = List.of(
            new HttpRoute(SignalCodec.TRACES, OtlpHttpPaths.TRACES_PATH),
            new HttpRoute(SignalCodec.METRICS, OtlpHttpPaths.METRICS_PATH),
            new HttpRoute(SignalCodec.LOGS, OtlpHttpPaths.LOGS_PATH),
            new HttpRoute(SignalCodec.PROFILES, OtlpHttpPaths.PROFILES_PATH));

    private final OtlpReceiverConfig config;
    private final PayloadDispatch dispatchers;
    private final GatewayMetrics telemetry;
    private final Set<Protocol> protocols;
    private final ClientAuth clientAuth;

    private @Nullable Server server;
    private volatile @Nullable AdmissionControl admission;

    /// Full constructor.
    ///
    /// @param config      the transport config
    /// @param dispatchers the per-signal dispatch functions
    /// @param telemetry   the self-observability registry for admission health
    /// @param protocols   the protocols to serve (at least one of [Protocol#GRPC], [Protocol#HTTP])
    /// @param clientAuth  the client-certificate authentication mode (enforced mutual TLS)
    public ArmeriaOtlpServer(OtlpReceiverConfig config, PayloadDispatch dispatchers, GatewayMetrics telemetry,
            Set<Protocol> protocols, ClientAuth clientAuth) {
        if (protocols.isEmpty()) {
            throw new IllegalArgumentException("at least one protocol must be served");
        }
        this.config = config;
        this.dispatchers = dispatchers;
        this.telemetry = telemetry;
        this.protocols = EnumSet.copyOf(protocols);
        this.clientAuth = clientAuth;
    }

    /// Full constructor defaulting to [ClientAuth#NONE].
    ///
    /// @param config      the transport config
    /// @param dispatchers the per-signal dispatch functions
    /// @param telemetry   the self-observability registry for admission health
    /// @param protocols   the protocols to serve (at least one of [Protocol#GRPC], [Protocol#HTTP])
    public ArmeriaOtlpServer(OtlpReceiverConfig config, PayloadDispatch dispatchers, GatewayMetrics telemetry,
            Set<Protocol> protocols) {
        this(config, dispatchers, telemetry, protocols, ClientAuth.NONE);
    }

    /// Convenience constructor with no self-observability ([GatewayMetrics#noop]) and [ClientAuth#NONE].
    ///
    /// @param config      the transport config
    /// @param dispatchers the per-signal dispatch functions
    /// @param protocols   the protocols to serve
    public ArmeriaOtlpServer(OtlpReceiverConfig config, PayloadDispatch dispatchers, Set<Protocol> protocols) {
        this(config, dispatchers, GatewayMetrics.noop(), protocols, ClientAuth.NONE);
    }

    public synchronized void start() throws IOException {
        if (server != null) {
            throw new IllegalStateException("server already started");
        }
        var address = Transports.bindAddress(config.bindHost(), config.port());
        var sb = Server.builder();
        if (config.tls() instanceof Tls.Disabled) {
            sb.http(address);
        } else {
            sb.https(address);
            ArmeriaTls.applyServer(sb, config.tls(), clientAuth);
        }
        // Leave the server-level wire request-length limit unbounded so the gRPC and HTTP caps stay
        // decoupled: gRPC enforces its decoded-message cap through the GrpcService message-length cap,
        // so an oversize gRPC message maps cleanly to RESOURCE_EXHAUSTED. The HTTP routes below instead
        // carry their own per-route wire cap, so an oversize HTTP body is rejected at the framing layer
        // before Armeria aggregates it into heap, rather than after.
        sb.maxRequestLength(0);
        sb.gracefulShutdownTimeout(QUIET_PERIOD, DRAIN_WINDOW);

        var admissionControl = AdmissionControl.of(config, telemetry);
        this.admission = admissionControl;

        if (protocols.contains(Protocol.GRPC)) {
            var grpc = GrpcService.builder()
                    .maxRequestMessageLength(config.maxInboundMessageSizeBytes())
                    // Invoke the OTLP service methods on Armeria's blocking-task executor rather than
                    // the I/O event loop, so a blocking SignalConsumer cannot stall the shared event
                    // loop and every other connection multiplexed onto it.
                    .useBlockingTaskExecutor(true);
            GrpcServiceAdapters.create(dispatchers, admissionControl).forEach(grpc::addService);
            sb.service(grpc.build());
        }
        if (protocols.contains(Protocol.HTTP)) {
            // Cap the wire request length on each HTTP route at the decoded-size limit, so Armeria
            // rejects an oversize body at the framing layer before aggregating it into heap. A gzip
            // body is bounded here by its compressed size (never larger than the decoded size it must
            // stay under), and the decoded-size inflation cap in the request mapping still applies.
            var wireCap = (long) config.maxInboundMessageSizeBytes();
            for (var route : HTTP_ROUTES) {
                sb.route().post(route.path())
                        .maxRequestLength(wireCap)
                        .build(HttpProtocolMapping.service(
                                route.codec(), dispatchers, config.maxInboundMessageSizeBytes(), admissionControl));
            }
        }

        var built = sb.build();
        try {
            built.start().join();
        } catch (CompletionException e) {
            var cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IOException("failed to start OTLP Armeria server", cause);
        }
        this.server = built;
        log.info("OTLP Armeria server ({}) listening on port {}", protocols, built.activeLocalPort());
    }

    public synchronized int port() {
        return server == null ? 0 : server.activeLocalPort();
    }

    public CompletionStage<Void> shutdown(Duration timeout) {
        return stop(timeout);
    }

    public CompletionStage<Void> shutdownNow() {
        return stop(FORCEFUL_BOUND);
    }

    /// Stops the server, bounding the returned stage by `bound` while Armeria drains in-flight
    /// requests within its build-time [#DRAIN_WINDOW]. Closes the admission gauges first.
    private CompletionStage<Void> stop(Duration bound) {
        Server current;
        synchronized (this) {
            current = server;
        }
        if (current == null) {
            return CompletableFuture.completedFuture(null);
        }
        closeAdmission();
        return current.stop()
                .completeOnTimeout(null, Math.max(0, bound.toMillis()), TimeUnit.MILLISECONDS);
    }

    /// Closes the admission control (idempotent) so its in-flight gauge registrations are released on
    /// shutdown; a null admission (server never started) is a no-op.
    private void closeAdmission() {
        var current = admission;
        if (current != null) {
            current.close();
        }
    }
}
