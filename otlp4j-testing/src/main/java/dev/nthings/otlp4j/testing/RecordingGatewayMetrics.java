package dev.nthings.otlp4j.testing;

import dev.nthings.otlp4j.core.telemetry.GatewayMetrics;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/// An in-memory recording [GatewayMetrics] for tests: counters accumulate in an [AtomicLong] and
/// gauges hold their live [LongSupplier] until unregistered, so a test can assert an instrument's
/// value after driving the component that writes it. No logging, so the testing module carries no
/// logging dependency. Thread-safe: the maps are concurrent and counter state is atomic.
public final class RecordingGatewayMetrics implements GatewayMetrics {

    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final Map<String, LongSupplier> gauges = new ConcurrentHashMap<>();

    @Override
    public LongCounter counter(String name, String description) {
        var total = counters.computeIfAbsent(name, ignored -> new AtomicLong());
        return total::addAndGet;
    }

    @Override
    public Registration gauge(String name, String description, LongSupplier value) {
        gauges.put(name, value);
        return () -> gauges.remove(name, value);
    }

    /// The current accumulated value of counter `name`, or `0` if it was never incremented.
    ///
    /// @param name the `otlp4j.*` counter name
    /// @return the accumulated count
    public long counterValue(String name) {
        var total = counters.get(name);
        return total == null ? 0L : total.get();
    }

    /// The current sampled value of gauge `name`, or `0` if no such gauge is registered.
    ///
    /// @param name the `otlp4j.*` gauge name
    /// @return the sampled value
    public long gaugeValue(String name) {
        var supplier = gauges.get(name);
        return supplier == null ? 0L : supplier.getAsLong();
    }
}
