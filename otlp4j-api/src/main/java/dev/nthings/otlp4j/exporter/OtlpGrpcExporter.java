package dev.nthings.otlp4j.exporter;

import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ExportResult;
import dev.nthings.otlp4j.api.internal.SpiSupport;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.spi.OtlpClientProvider;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Exports typed telemetry to an OTLP endpoint.
///
/// The client implementation is supplied by an [OtlpClientProvider] discovered through
/// [SpiSupport]. Add `otlp4j-transport` at runtime for the built-in OTLP/gRPC transport.
/// Use this as a terminal [dev.nthings.otlp4j.pipeline.TelemetryConsumer].
public final class OtlpGrpcExporter implements Exporter {

    private static final Logger log = LoggerFactory.getLogger(OtlpGrpcExporter.class);

    private final OtlpClient client;

    private OtlpGrpcExporter(Builder builder) {
        this.client = SpiSupport.provider(OtlpClientProvider.class)
                .create(builder.host, builder.port, builder.timeout);
        log.debug("created OTLP/gRPC exporter for endpoint {}:{}", builder.host, builder.port);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public ExportResult consumeTraces(TraceData traces) {
        return client.exportTraces(traces);
    }

    @Override
    public ExportResult consumeMetrics(MetricsData metrics) {
        return client.exportMetrics(metrics);
    }

    @Override
    public ExportResult consumeLogs(LogsData logs) {
        return client.exportLogs(logs);
    }

    @Override
    public ExportResult consumeProfiles(ProfilesData profiles) {
        return client.exportProfiles(profiles);
    }

    @Override
    public void close() {
        client.close();
    }

    /// Builds an [OtlpGrpcExporter]. Defaults to `localhost:4317` with a 10s timeout.
    public static final class Builder {

        private String host = "localhost";
        private int port = 4317;
        private Duration timeout = Duration.ofSeconds(10);

        private Builder() {}

        /// Sets the target host and port in one call.
        public Builder endpoint(String host, int port) {
            this.host = host;
            this.port = port;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /// Sets the per-export deadline.
        public Builder timeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public OtlpGrpcExporter build() {
            return new OtlpGrpcExporter(this);
        }
    }
}
