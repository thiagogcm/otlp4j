package dev.nthings.otlp4j.transport.common;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.Signal;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.Sink;
import dev.nthings.otlp4j.pipeline.Source;
import dev.nthings.otlp4j.spi.OtlpServer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Covers the generic [Signal]-keyed accessors introduced by the F3 spine: [ClientExporter#sink],
/// [ServerReceiver#source], and the tap's `publisher`. Each must resolve to the same per-signal
/// instance the typed veneer returns, and the exporter sink must export through the shared client.
@DisplayName("Generic signal accessors")
class SignalAccessorsTest {

    /// Inert transport server; never binds a port.
    private static final class StubServer implements OtlpServer {

        @Override
        public void start() {}

        @Override
        public int port() {
            return 0;
        }

        @Override
        public CompletionStage<Void> shutdown(Duration timeout) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> shutdownNow() {
            return CompletableFuture.completedFuture(null);
        }
    }

    @DisplayName("exporter.sink(Signal) resolves to the matching facet and exports through it")
    @Test
    void exporterSinkResolvesAndExports() {
        var exporter = new ClientExporter(new RecordingOtlpClient(), "test");
        Sink<MetricsData> sink = exporter.sink(Signal.METRICS);

        assertThat(sink).isSameAs(exporter.metrics());
        var result = sink.consume(Signal.METRICS.empty()).toCompletableFuture().join();
        assertThat(result).isInstanceOf(ConsumeResult.Accepted.class);
    }

    @DisplayName("receiver.source(Signal) resolves to the matching per-signal source")
    @Test
    void receiverSourceResolves() {
        var receiver = new ServerReceiver("OTLP/test", disp -> new StubServer(), null, null, null, null, dev.nthings.otlp4j.telemetry.GatewayMetrics.noop());
        Source<LogsData> source = receiver.source(Signal.LOGS);
        assertThat(source).isSameAs(receiver.logs());
    }

    @DisplayName("tap.publisher(Signal) resolves to the matching per-signal publisher")
    @Test
    void tapPublisherResolves() {
        var receiver = new ServerReceiver("OTLP/test", disp -> new StubServer(), null, null, null, null, dev.nthings.otlp4j.telemetry.GatewayMetrics.noop());
        Flow.Publisher<TracesData> publisher = receiver.tap().publisher(Signal.TRACES);
        assertThat(publisher).isSameAs(receiver.tap().traces());
    }
}
