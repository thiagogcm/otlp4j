package dev.nthings.otlp4j.transport.common;

import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.pipeline.Source;
import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Signal;
import dev.nthings.otlp4j.model.SignalBatch;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.receiver.Receiver;
import dev.nthings.otlp4j.receiver.TelemetryTap;
import dev.nthings.otlp4j.spi.Dispatchers;
import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Shared [Receiver] implementation backed by a transport [OtlpServer]. Exposes per-signal
/// [Source]s and a [TelemetryTap], dispatches decoded batches to subscribers while mirroring
/// to the tap. gRPC and HTTP entry points compose one of these.
public final class ServerReceiver implements Receiver, Dispatchers {

    private static final Logger log = LoggerFactory.getLogger(ServerReceiver.class);

    private final Map<Signal<?>, SignalSource<?>> sources = new LinkedHashMap<>();
    private final ReceiverTap tap;
    private final OtlpServer server;
    private final String transportName;

    /// Builds dispatch wiring, then asks `serverFactory` for the transport server bound to it.
    /// `transportName` (e.g. `OTLP/gRPC`) is used only for log messages. `telemetry` feeds the tap's
    /// `otlp4j.receiver.tap.dropped` counter.
    public ServerReceiver(
            String transportName,
            Function<Dispatchers, OtlpServer> serverFactory,
            @Nullable Sink<? super TracesData> onTraces,
            @Nullable Sink<? super MetricsData> onMetrics,
            @Nullable Sink<? super LogsData> onLogs,
            @Nullable Sink<? super ProfilesData> onProfiles,
            GatewayMetrics telemetry) {
        this.transportName = transportName;
        this.tap = new ReceiverTap(telemetry);
        for (Signal<?> signal : Signal.values()) {
            sources.put(signal, new SignalSource<>(signal));
        }
        this.server = serverFactory.apply(this);
        subscribeIfPresent(Signal.TRACES, onTraces);
        subscribeIfPresent(Signal.METRICS, onMetrics);
        subscribeIfPresent(Signal.LOGS, onLogs);
        subscribeIfPresent(Signal.PROFILES, onProfiles);
    }

    /// The source for `signal`. The map is keyed by [Signal] identity and each entry was created for
    /// that signal, so the value's element type is exactly the signal's batch type.
    private <T> SignalSource<T> sourceFor(Signal<T> signal) {
        @SuppressWarnings("unchecked")
        var source = (SignalSource<T>) sources.get(signal);
        return source;
    }

    private <T> void subscribeIfPresent(Signal<T> signal, @Nullable Sink<? super T> sink) {
        if (sink != null) {
            sourceFor(signal).subscribe(sink);
        }
    }

    @Override
    public <T> Source<T> source(Signal<T> signal) {
        return sourceFor(signal);
    }

    @Override
    public TelemetryTap tap() { return tap; }

    @Override
    public Receiver start() {
        // Starts the underlying transport; throws [UncheckedIOException] if bind fails.
        try {
            server.start();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to start " + transportName + " receiver", e);
        }
        log.debug("{} receiver started on port {}", transportName, server.port());
        return this;
    }

    @Override
    public int port() {
        return server.port();
    }

    @Override
    public CompletionStage<Void> shutdown(Duration timeout) {
        // Graceful: drain server first, then close tap. Closing it up front makes
        // [MulticastPublisher#publish] early-return, dropping telemetry accepted during drain.
        return server.shutdown(timeout).whenComplete((v, t) -> tap.close());
    }

    @Override
    public CompletionStage<Void> shutdownNow() {
        // Forceful: nothing further is accepted, so detach subscribers immediately.
        tap.close();
        return server.shutdownNow();
    }

    @Override
    public <T> CompletionStage<ConsumeResult> dispatch(SignalBatch<T> batch) {
        // Tap before source, matching the former per-signal dispatchX order exactly.
        tap.publish(batch);
        return sourceFor(batch.signal()).dispatch(batch.batch());
    }
}
