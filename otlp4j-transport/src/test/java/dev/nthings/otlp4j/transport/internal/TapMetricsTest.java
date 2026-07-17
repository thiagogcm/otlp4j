package dev.nthings.otlp4j.transport.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.core.policy.OverflowPolicy;
import dev.nthings.otlp4j.codec.Payloads;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.testing.RecordingGatewayMetrics;
import dev.nthings.otlp4j.testing.Fixtures;
import dev.nthings.otlp4j.testing.FlowSubscribers;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Pins F11's tap counter: `otlp4j.receiver.tap.dropped` is fed alongside [ReceiverTap#droppedCount].
@DisplayName("ReceiverTap self-observability")
class TapMetricsTest {

    private static final String DROPPED = "otlp4j.receiver.tap.dropped";

    @DisplayName("a slow subscriber's overflow drops feed the tap dropped counter")
    @Test
    void tapDropsFeedTheCounter() {
        var registry = new RecordingGatewayMetrics();
        try (var tap = new ReceiverTap(registry)) {
            // A no-demand subscriber with a 1-slot DROP_NEWEST buffer: everything past the first
            // buffered item is dropped.
            tap.publisher(SignalDescriptor.TRACES, new TapOptions(OverflowPolicy.DROP_NEWEST, 1))
                    .subscribe(FlowSubscribers.noOp());

            for (var i = 0; i < 20; i++) {
                var data = Fixtures.traceData(Fixtures.span("s" + i, Span.Kind.SERVER));
                tap.publish(Payloads.domain(SignalDescriptor.TRACES, data));
            }

            await().atMost(Duration.ofSeconds(2)).until(() -> tap.droppedCount() > 0);
            assertThat(registry.counterValue(DROPPED)).isEqualTo(tap.droppedCount());
        }
    }
}
