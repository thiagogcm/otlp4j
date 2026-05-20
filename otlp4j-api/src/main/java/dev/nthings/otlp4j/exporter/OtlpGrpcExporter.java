package dev.nthings.otlp4j.exporter;

import dev.nthings.otlp4j.api.internal.SpiSupport;
import dev.nthings.otlp4j.pipeline.LogConsumer;
import dev.nthings.otlp4j.pipeline.MetricConsumer;
import dev.nthings.otlp4j.pipeline.ProfileConsumer;
import dev.nthings.otlp4j.pipeline.TraceConsumer;
import dev.nthings.otlp4j.spi.ClientTransportConfig;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.spi.OtlpClientProvider;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Exports typed telemetry to an OTLP/gRPC endpoint.
///
/// One instance handles all four signals. The per-signal facets ([#traces()], [#metrics()],
/// [#logs()], [#profiles()]) are typed `Consumer`s the pipeline attaches to; lifecycle lives on
/// the exporter itself.
public final class OtlpGrpcExporter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OtlpGrpcExporter.class);

    private final OtlpClient client;

    private OtlpGrpcExporter(Builder b) {
        var cfg = b.config.build();
        this.client = SpiSupport.firstProvider(OtlpClientProvider.class).create(cfg);
        log.debug("created OTLP/gRPC exporter for endpoint {}:{}", cfg.host(), cfg.port());
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Convenience constructor: build, connect, ready-to-use.
    public static OtlpGrpcExporter to(String host, int port) {
        return builder().endpoint(host, port).build();
    }

    public TraceConsumer    traces()   { return client::exportTraces; }
    public MetricConsumer   metrics()  { return client::exportMetrics; }
    public LogConsumer      logs()     { return client::exportLogs; }
    public ProfileConsumer  profiles() { return client::exportProfiles; }

    public CompletionStage<Void> forceFlush(Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }

    /// Closes the underlying transport on a worker thread; completes on close or `timeout`.
    public CompletionStage<Void> shutdown(Duration timeout) {
        return CompletableFuture
                .runAsync(client::close)
                .orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void close() {
        shutdown(Duration.ofSeconds(10)).toCompletableFuture().join();
    }

    /// Builder for [OtlpGrpcExporter]. Defaults to `localhost:4317` with a 10s deadline.
    public static final class Builder {

        private ClientTransportConfig.Builder config = ClientTransportConfig.builder();

        private Builder() {}

        public Builder transport(ClientTransportConfig config) {
            this.config = config.toBuilder();
            return this;
        }

        public Builder endpoint(String host, int port) {
            config.endpoint(host, port);
            return this;
        }

        public Builder host(String host) {
            config.host(host);
            return this;
        }

        public Builder port(int port) {
            config.port(port);
            return this;
        }

        public Builder timeout(Duration timeout) {
            config.timeout(timeout);
            return this;
        }

        public OtlpGrpcExporter build() {
            return new OtlpGrpcExporter(this);
        }
    }
}
