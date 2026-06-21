package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.model.LogRecord;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TraceData;
import dev.nthings.otlp4j.pipeline.Telemetry;
import dev.nthings.otlp4j.testing.Fixtures;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Telemetry envelope")
class TelemetryEnvelopeTest {

    @DisplayName("Each Telemetry variant exposes its wrapped payload")
    @Test
    void everyVariantWrapsItsPayload() {
        var traces = Fixtures.traceData(Fixtures.span("a", Span.Kind.SERVER));
        var metrics = Fixtures.metricsData(Fixtures.metric("m"));
        var logs = Fixtures.logsData(Fixtures.logRecord("hi", LogRecord.Severity.INFO));
        var profiles = Fixtures.profilesData(Fixtures.profile("p"));

        var t = new Telemetry.Traces(traces);
        var m = new Telemetry.Metrics(metrics);
        var l = new Telemetry.Logs(logs);
        var p = new Telemetry.Profiles(profiles);

        assertThat(t.data()).isSameAs(traces);
        assertThat(m.data()).isSameAs(metrics);
        assertThat(l.data()).isSameAs(logs);
        assertThat(p.data()).isSameAs(profiles);
    }

    @DisplayName("Sealed Telemetry switches exhaustively over all variants")
    @Test
    void sealedExhaustiveSwitch() {
        List<Telemetry> items = List.of(
                new Telemetry.Traces(new TraceData(List.of())),
                new Telemetry.Metrics(new MetricsData(List.of())),
                new Telemetry.Logs(new LogsData(List.of())),
                new Telemetry.Profiles(new ProfilesData(List.of())));
        for (var item : items) {
            String label = switch (item) {
                case Telemetry.Traces t -> "T";
                case Telemetry.Metrics m -> "M";
                case Telemetry.Logs l -> "L";
                case Telemetry.Profiles p -> "P";
            };
            assertThat(label).isIn("T", "M", "L", "P");
        }
    }
}
