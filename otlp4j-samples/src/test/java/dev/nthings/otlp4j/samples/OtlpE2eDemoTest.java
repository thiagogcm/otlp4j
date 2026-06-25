package dev.nthings.otlp4j.samples;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// End-to-end sample canary: runs the demo and asserts what reached the backend.
@Timeout(30)
@DisplayName("OTLP end-to-end demo")
class OtlpE2eDemoTest {

    @DisplayName("demo filters spans, derives count, and enriches the resource")
    @Test
    void demoTraversesTheFullPipeline() throws Exception {
        var result = OtlpE2eDemo.run();

        assertThat(result.spansAtBackend())
                .as("only the 3 SERVER spans should survive the filter")
                .isEqualTo(3);
        assertThat(result.derivedSpanCount())
                .as("the CountConnector should derive a span-count metric of 3")
                .isEqualTo(3L);
        assertThat(result.enrichedEnvironment())
                .as("the processor should have stamped deployment.environment onto the resource")
                .isEqualTo("demo");
    }
}
