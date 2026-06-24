package dev.nthings.otlp4j.exporter;

import dev.nthings.otlp4j.core.Drainable;
import dev.nthings.otlp4j.core.Flushable;
import dev.nthings.otlp4j.core.LogSink;
import dev.nthings.otlp4j.core.MetricSink;
import dev.nthings.otlp4j.core.ProfileSink;
import dev.nthings.otlp4j.core.Sink;
import dev.nthings.otlp4j.core.TraceSink;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.spi.OtlpClient;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Shared lifecycle for the OTLP exporters: one instance handles all four signals through the typed
/// facets ([#traces()], [#metrics()], [#logs()], [#profiles()]). A concrete subclass (in a transport
/// module) supplies the [OtlpClient] it speaks through; the facets, flush and shutdown live here.
///
/// Each facet carries the exporter's lifecycle: it delivers to the client but is itself [Drainable]
/// and [Flushable], delegating both back here, so a pipeline terminating in (or fanning out to)
/// `exporter.traces()` auto-owns the exporter without `Stage.owns(exporter)` or a two-arg
/// `to(..., exporter)`. Shutdown is cancellation-aware: the caller's deadline interrupts the transport
/// teardown rather than leaving it blocking in the background.
public abstract class AbstractOtlpExporter implements Drainable, Flushable {

    private static final Logger log = LoggerFactory.getLogger(AbstractOtlpExporter.class);

    private final OtlpClient client;

    /// A single-thread platform executor this exporter owns, so a shutdown timeout can `shutdownNow()`
    /// it to interrupt the blocking transport close (whose channel/executor awaits are interrupt-aware).
    private final ExecutorService shutdownExecutor = Executors.newSingleThreadExecutor(
            Thread.ofPlatform().daemon().name("otlp-exporter-shutdown").factory());

    /// Wraps the transport `client` a concrete exporter has built for its protocol.
    protected AbstractOtlpExporter(OtlpClient client) {
        this.client = client;
    }

    public final TraceSink    traces()   { return new TraceFacet(this, client::exportTraces); }
    public final MetricSink   metrics()  { return new MetricFacet(this, client::exportMetrics); }
    public final LogSink      logs()     { return new LogFacet(this, client::exportLogs); }
    public final ProfileSink  profiles() { return new ProfileFacet(this, client::exportProfiles); }

    /// No-op: the exporter holds no buffer. Still reachable via a pipeline `forceFlush` once owned.
    @Override
    public final CompletionStage<Void> forceFlush(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }

    /// Closes the transport on this exporter's own shutdown thread, completing on a clean close or
    /// `timeout`; on timeout we `shutdownNow()` the executor to interrupt the transport's awaits.
    ///
    /// Idempotent: once closed the executor is shut down, so a repeat call sees a rejected submission
    /// and returns completed — an owned exporter can be drained by both the subscription and `close()`.
    @Override
    public final CompletionStage<Void> shutdown(Duration timeout) {
        CompletableFuture<Void> closing;
        try {
            closing = CompletableFuture.runAsync(client::close, shutdownExecutor);
        } catch (RejectedExecutionException alreadyClosed) {
            return CompletableFuture.completedFuture(null);
        }
        closing = closing.orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
        return closing.whenComplete((v, e) -> {
            if (e instanceof TimeoutException) {
                log.warn("exporter close exceeded {}; interrupting transport teardown", timeout);
                shutdownExecutor.shutdownNow(); // interrupts client.close() (channel/executor awaits respond to interrupt)
            } else {
                shutdownExecutor.shutdown();
            }
        });
    }

    // close() is inherited from Drainable: shutdown(10s) on this exporter's own shutdown thread.

    /// A per-signal sink view whose lifecycle is the owning exporter's: `consume` delivers to the
    /// client, while [#shutdown]/[#forceFlush] delegate to the exporter. Being [AutoCloseable] (via
    /// [Drainable]), it is auto-collected as a pipeline terminal or fan-out peer. The delegation is
    /// idempotent, so also registering the exporter via `owns(...)` stays harmless.
    private abstract static class OwnedFacet<T> implements Sink<T>, Drainable, Flushable {

        private final AbstractOtlpExporter owner;
        private final Sink<T> delegate;

        private OwnedFacet(AbstractOtlpExporter owner, Sink<T> delegate) {
            this.owner = owner;
            this.delegate = delegate;
        }

        @Override
        public final CompletionStage<ConsumeResult<T>> consume(T batch) {
            return delegate.consume(batch);
        }

        @Override
        public final CompletionStage<Void> shutdown(Duration timeout) {
            return owner.shutdown(timeout);
        }

        @Override
        public final CompletionStage<Void> forceFlush(Duration timeout) {
            return owner.forceFlush(timeout);
        }

        // close() inherited from Drainable: shutdown(10s) → owner.shutdown(10s).
    }

    private static final class TraceFacet extends OwnedFacet<TraceData> implements TraceSink {
        private TraceFacet(AbstractOtlpExporter owner, Sink<TraceData> delegate) { super(owner, delegate); }
    }

    private static final class MetricFacet extends OwnedFacet<MetricsData> implements MetricSink {
        private MetricFacet(AbstractOtlpExporter owner, Sink<MetricsData> delegate) { super(owner, delegate); }
    }

    private static final class LogFacet extends OwnedFacet<LogsData> implements LogSink {
        private LogFacet(AbstractOtlpExporter owner, Sink<LogsData> delegate) { super(owner, delegate); }
    }

    private static final class ProfileFacet extends OwnedFacet<ProfilesData> implements ProfileSink {
        private ProfileFacet(AbstractOtlpExporter owner, Sink<ProfilesData> delegate) { super(owner, delegate); }
    }
}
