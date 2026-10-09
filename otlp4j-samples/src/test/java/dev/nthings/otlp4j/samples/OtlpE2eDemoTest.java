package dev.nthings.otlp4j.samples;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// End-to-end sample canary: runs the demo and asserts what reached the backend.
@Timeout(30)
@DisplayName("OTLP end-to-end demo")
class OtlpE2eDemoTest {

    @DisplayName("demo drops a resource, filters spans, derives count, enriches the resource, and redacts")
    @Test
    void demoTraversesTheFullPipeline() throws Exception {
        var result = OtlpE2eDemo.run();

        assertThat(result.spansAtBackend())
                .as("only checkout's 3 SERVER spans should survive: the resource filter drops the "
                        + "monitor's 2 SERVER spans and the span filter drops the 2 INTERNAL spans")
                .isEqualTo(3);
        assertThat(result.derivedSpanCount())
                .as("the span-count connector should count only the 3 retained spans")
                .isEqualTo(3L);
        assertThat(result.enrichedEnvironment())
                .as("the processor should have stamped deployment.environment onto the resource")
                .isEqualTo("demo");
        assertThat(result.redactedEnduserId())
                .as("the redaction should have masked the sensitive enduser.id attribute")
                .isEqualTo("***");
    }
}
