package dev.nthings.otlp4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.nthings.otlp4j.model.ConsumeResult;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.pipeline.TracesSink;
import dev.nthings.otlp4j.pipeline.Wiring;
import dev.nthings.otlp4j.pipeline.ops.BatchScheduler;
import dev.nthings.otlp4j.pipeline.ops.Batcher;
import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("BatchScheduler")
class BatchSchedulerTest {

    private static final TracesSink OK = traces -> ConsumeResult.acceptedStage();

    /// Builds a batcher on `scheduler`; constructing it schedules the fixed-rate flush, which starts
    /// the scheduler's single daemon timer thread. The caller closes the batcher.
    private static Batcher<TracesData> batcherOn(BatchScheduler scheduler) {
        return Batcher.forTraces()
                .downstream(OK)
                .scheduler(scheduler)
                .flushInterval(Duration.ofMillis(200))
                .build();
    }

    private static boolean threadAlive(String name) {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(t -> t.isAlive() && t.getName().equals(name));
    }

    private static long threadsNamed(String name) {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.isAlive() && t.getName().equals(name))
                .count();
    }

    @DisplayName("Owned schedulers name distinct timer threads and close() terminates them")
    @Test
    @Timeout(15)
    void ownedSchedulerIsolatesAndCloses() {
        var t1 = BatchScheduler.create("gw1-batch-timer");
        var t2 = BatchScheduler.create("gw2-batch-timer");
        var b1 = batcherOn(t1);
        var b2 = batcherOn(t2);
        try {
            // Each owned scheduler runs its flush on its own distinctly named daemon thread.
            await().atMost(Duration.ofSeconds(5)).until(() -> threadAlive("gw1-batch-timer"));
            await().atMost(Duration.ofSeconds(5)).until(() -> threadAlive("gw2-batch-timer"));
        } finally {
            b1.close();
            b2.close();
        }
        // Closing an owned scheduler terminates its timer thread; its sibling is unaffected.
        t1.close();
        await().atMost(Duration.ofSeconds(5)).until(() -> !threadAlive("gw1-batch-timer"));
        assertThat(threadAlive("gw2-batch-timer")).isTrue();
        t2.close();
        await().atMost(Duration.ofSeconds(5)).until(() -> !threadAlive("gw2-batch-timer"));
    }

    @DisplayName("shared().close() is a no-op: the otlp4j-batch-timer thread survives")
    @Test
    @Timeout(15)
    void sharedCloseIsNoOp() {
        // A batcher built with no explicit scheduler falls back to the shared singleton.
        var batcher = Batcher.forTraces()
                .downstream(OK)
                .flushInterval(Duration.ofMillis(200))
                .build();
        try {
            await().atMost(Duration.ofSeconds(5)).until(() -> threadAlive("otlp4j-batch-timer"));
            // close() on the shared singleton must not tear its thread down.
            BatchScheduler.shared().close();
            assertThat(threadAlive("otlp4j-batch-timer")).isTrue();
            // Exactly one shared thread regardless of how many fallback batchers coexist.
            assertThat(threadsNamed("otlp4j-batch-timer")).isEqualTo(1L);
        } finally {
            batcher.close();
        }
        // Still alive after the batcher is gone: the shared timer outlives every batcher.
        assertThat(threadAlive("otlp4j-batch-timer")).isTrue();
    }

    @DisplayName("create() names its thread otlp4j-batch-timer-owned-*, disjoint from the singleton")
    @Test
    @Timeout(15)
    void createUsesDistinctThreadName() {
        var owned = BatchScheduler.create();
        var batcher = batcherOn(owned);
        try {
            await().atMost(Duration.ofSeconds(5)).until(() -> !ownedSuffixThreadNames().isEmpty());
            // The auto-named owned thread carries the -owned-N suffix, so it can never collide with
            // the shared singleton's literal "otlp4j-batch-timer" (the guard for batchersShareOne...).
            assertThat(ownedSuffixThreadNames()).allMatch(n -> !n.equals("otlp4j-batch-timer"));
        } finally {
            batcher.close();
        }
        owned.close();
    }

    @DisplayName("A scheduler registered with Wiring.owns is closed on Wiring teardown")
    @Test
    @Timeout(15)
    void wiringOwnsClosesScheduler() {
        var timer = BatchScheduler.create("wired-batch-timer");
        var batcher = batcherOn(timer);
        // The user creates the scheduler, wires each batcher to it, and hands ownership to Wiring
        // via the existing owns(AutoCloseable) surface (no new Wiring code, per design 2.2).
        try (var wiring = Wiring.builder().owns(batcher).owns(timer).build()) {
            await().atMost(Duration.ofSeconds(5)).until(() -> threadAlive("wired-batch-timer"));
            assertThat(wiring).isNotNull();
        }
        // Wiring's phase-3 owned close ran the scheduler's close(), terminating its timer thread.
        await().atMost(Duration.ofSeconds(5)).until(() -> !threadAlive("wired-batch-timer"));
    }

    private static Set<String> ownedSuffixThreadNames() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .map(Thread::getName)
                .filter(n -> n.startsWith("otlp4j-batch-timer-owned-"))
                .collect(java.util.stream.Collectors.toSet());
    }
}
