package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.telemetry.GatewayMetrics;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/// A tiny in-memory metrics registry for asserting counter increments in tests.
final class RecordingMetrics implements GatewayMetrics {

    private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();

    @Override
    public LongCounter counter(String name, String description) {
        var counter = counters.computeIfAbsent(name, ignored -> new AtomicLong());
        return counter::addAndGet;
    }

    @Override
    public Registration gauge(String name, String description, LongSupplier value) {
        return () -> {};
    }

    long value(String name) {
        return counters.getOrDefault(name, new AtomicLong()).get();
    }
}
