package dev.nthings.otlp4j.samples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("LoggingGatewayMetrics")
class LoggingGatewayMetricsTest {

    @DisplayName("a counter of the same name shares one accumulator across handles")
    @Test
    void sameNameSharesAccumulator() {
        var registry = new LoggingGatewayMetrics();
        var first = registry.counter("otlp4j.batcher.dropped", "drops");
        var second = registry.counter("otlp4j.batcher.dropped", "drops");

        first.add(2);
        second.add(3);
        assertThat(registry.counterValue("otlp4j.batcher.dropped")).isEqualTo(5);
    }

    @DisplayName("an unknown counter or gauge reads zero")
    @Test
    void unknownInstrumentsReadZero() {
        var registry = new LoggingGatewayMetrics();
        assertThat(registry.counterValue("otlp4j.missing")).isZero();
        assertThat(registry.gaugeValue("otlp4j.missing")).isZero();
    }

    @DisplayName("a re-registered gauge under the same name close does not evict the newer supplier")
    @Test
    void gaugeCloseIsValueScoped() {
        var registry = new LoggingGatewayMetrics();
        var older = registry.gauge("otlp4j.batcher.queued", "depth", () -> 1L);
        var newer = registry.gauge("otlp4j.batcher.queued", "depth", () -> 2L);

        // Closing the older registration must not remove the newer supplier (value-scoped remove).
        older.close();
        assertThat(registry.gaugeValue("otlp4j.batcher.queued")).isEqualTo(2);
        newer.close();
        assertThat(registry.gaugeValue("otlp4j.batcher.queued")).isZero();
    }

    @DisplayName("logSnapshot emits every instrument without throwing")
    @Test
    void logSnapshotDoesNotThrow() {
        var registry = new LoggingGatewayMetrics();
        registry.counter("otlp4j.exporter.export.success", "ok").add(1);
        var live = new AtomicLong(5);
        registry.gauge("otlp4j.receiver.admission.in_flight.requests", "reqs", live::get);
        assertThatCode(registry::logSnapshot).doesNotThrowAnyException();
    }
}
