package dev.nthings.otlp4j;

import static dev.nthings.otlp4j.testing.Fixtures.logRecord;
import static dev.nthings.otlp4j.testing.Fixtures.logsData;
import static dev.nthings.otlp4j.testing.Fixtures.metric;
import static dev.nthings.otlp4j.testing.Fixtures.metricsData;
import static dev.nthings.otlp4j.testing.Fixtures.profile;
import static dev.nthings.otlp4j.testing.Fixtures.profilesData;
import static dev.nthings.otlp4j.testing.Fixtures.span;
import static dev.nthings.otlp4j.testing.Fixtures.traceData;
import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.ExportResult;
import dev.nthings.otlp4j.pipeline.ForwardingConsumer;
import dev.nthings.otlp4j.pipeline.TelemetryConsumer;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/// Unit tests for `ForwardingConsumer` — a bare subclass must pass every signal through to its
/// delegate unchanged, and the `delegate()` accessor must return the wrapped consumer.
class ForwardingConsumerTest {

    @Test
    void aBareSubclassForwardsEverySignalUnchanged() {
        var traces = new AtomicReference<TraceData>();
        var metrics = new AtomicReference<MetricsData>();
        var logs = new AtomicReference<LogsData>();
        var profiles = new AtomicReference<ProfilesData>();
        var delegate = new TelemetryConsumer() {
            @Override
            public ExportResult consumeTraces(TraceData t) {
                traces.set(t);
                return ExportResult.success();
            }

            @Override
            public ExportResult consumeMetrics(MetricsData m) {
                metrics.set(m);
                return ExportResult.success();
            }

            @Override
            public ExportResult consumeLogs(LogsData l) {
                logs.set(l);
                return ExportResult.success();
            }

            @Override
            public ExportResult consumeProfiles(ProfilesData p) {
                profiles.set(p);
                return ExportResult.success();
            }
        };
        var forwarding = new ForwardingConsumer(delegate) {};

        var inTraces = traceData(span("op", Span.Kind.INTERNAL));
        var inMetrics = metricsData(metric("requests"));
        var inLogs = logsData(logRecord("hello", LogRecord.Severity.INFO));
        var inProfiles = profilesData(profile("abc"));

        forwarding.consumeTraces(inTraces);
        forwarding.consumeMetrics(inMetrics);
        forwarding.consumeLogs(inLogs);
        forwarding.consumeProfiles(inProfiles);

        assertThat(traces.get()).isSameAs(inTraces);
        assertThat(metrics.get()).isSameAs(inMetrics);
        assertThat(logs.get()).isSameAs(inLogs);
        assertThat(profiles.get()).isSameAs(inProfiles);
    }

    @Test
    void delegateAccessorReturnsTheWrappedConsumer() {
        var delegate = new TelemetryConsumer() {};
        var captured = new AtomicReference<TelemetryConsumer>();
        var forwarding = new ForwardingConsumer(delegate) {
            {
                captured.set(delegate());
            }
        };

        assertThat(forwarding).isNotNull();
        assertThat(captured.get()).isSameAs(delegate);
    }
}
