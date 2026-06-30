package dev.nthings.otlp4j.transport.spi;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.transport.spi.ClientExporter.LeakWatch;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Verifies the garbage-collection leak backstop: an exporter dropped without `shutdown()` (or
/// `close()`) reports a leak, while one that was shut down stays quiet.
@DisplayName("Exporter leak detection")
class ClientExporterLeakTest {

    @DisplayName("warns when an un-shut-down exporter is collected")
    @Test
    void warnsWhenNotShutDown() {
        var captured = new ArrayList<String>();
        var watch = new LeakWatch("com.example.DemoExporter", captured::add);

        watch.run();

        assertThat(captured).hasSize(1);
        assertThat(captured.get(0))
                .contains("com.example.DemoExporter")
                .contains("shutdown()")
                .contains("Stage.owns(exporter)");
    }

    @DisplayName("stays quiet once the exporter has been shut down")
    @Test
    void quietAfterShutdown() {
        var captured = new ArrayList<String>();
        var watch = new LeakWatch("com.example.DemoExporter", captured::add);

        watch.closed = true;
        watch.run();

        assertThat(captured).isEmpty();
    }

    @DisplayName("a real exporter dropped without shutdown is reported on GC")
    @Test
    void realExporterReportsOnGc() throws InterruptedException {
        var reported = new CopyOnWriteArrayList<String>();
        var latch = new CountDownLatch(1);
        Consumer<String> reporter = message -> {
            reported.add(message);
            latch.countDown();
        };

        abandonExporter(reporter);

        for (int i = 0; i < 50 && latch.getCount() > 0; i++) {
            System.gc();
            if (latch.await(100, TimeUnit.MILLISECONDS)) {
                break;
            }
        }

        assertThat(latch.getCount()).as("leak warning fired after GC").isZero();
        assertThat(reported.get(0)).contains("garbage-collected before shutdown()");
    }

    /// Builds an exporter and keeps no reference to it, so it is collectable once this returns.
    private static void abandonExporter(Consumer<String> reporter) {
        new ClientExporter(new RecordingOtlpClient(), "com.example.DemoExporter", reporter);
    }
}
