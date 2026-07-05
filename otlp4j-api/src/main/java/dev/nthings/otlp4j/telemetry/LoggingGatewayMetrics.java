package dev.nthings.otlp4j.telemetry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// A dependency-free sample [GatewayMetrics] adapter that keeps every instrument in memory and logs
/// through slf4j. Counters accumulate in an [AtomicLong] and log each delta at TRACE; gauges hold
/// their live [LongSupplier] until unregistered. Call [#logSnapshot] on a schedule to emit every
/// instrument's current value at INFO (an operator's periodic health line).
///
/// This is a reference adapter, not the recommended production sink: an OpenTelemetry-SDK adapter
/// (bridging [#counter] onto an OTel `LongCounter` and [#gauge] onto an observable gauge) belongs in
/// a sample module so the OTel dependency stays out of the API module. Thread-safe: the maps are
/// concurrent and counter state is atomic.
public final class LoggingGatewayMetrics implements GatewayMetrics {

    private static final Logger log = LoggerFactory.getLogger(LoggingGatewayMetrics.class);

    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final Map<String, LongSupplier> gauges = new ConcurrentHashMap<>();

    @Override
    public LongCounter counter(String name, String description) {
        var total = counters.computeIfAbsent(name, ignored -> new AtomicLong());
        return delta -> {
            var current = total.addAndGet(delta);
            log.trace("otlp4j metric {} += {} (now {})", name, delta, current);
        };
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

    /// Logs every counter's accumulated value and every registered gauge's sampled value at INFO.
    /// Intended to be called periodically by an operator schedule.
    public void logSnapshot() {
        counters.forEach((name, total) -> log.info("otlp4j metric {} = {}", name, total.get()));
        gauges.forEach((name, value) -> log.info("otlp4j metric {} = {}", name, value.getAsLong()));
    }
}
