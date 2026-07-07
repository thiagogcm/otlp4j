package dev.nthings.otlp4j.core.component;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/// The build-scoped shared-component cache the graph builder hands every factory through its
/// [CreateContext]. It realizes the shared-component guarantee: a component id that appears in more
/// than one pipeline is backed by exactly one underlying instance, and the several per-signal facets a
/// factory returns for that id all draw on that single instance rather than each opening its own.
///
/// This mirrors the Collector's `sharedcomponent` map, keyed here by [ComponentId]. A factory that
/// wants to share an expensive resource (an exporter's wire client, for example) builds it through
/// [#computeIfAbsent] rather than directly, so the second and later facets of the same id reuse the
/// instance the first facet built. One instance is created per id per gateway build; a fresh builder
/// starts with an empty cache, so nothing leaks across builds.
public final class SharedComponents {

    private final Map<ComponentId, Object> instances = new HashMap<>();

    /// Returns the shared instance registered under `id`, building it with `factory` on first request
    /// and reusing it on every later request for the same id within this build.
    ///
    /// The cast is safe because a given id is always resolved through the same factory in one build, so
    /// the stored instance always has the type the caller expects.
    ///
    /// @param id      the component id the instance is shared under
    /// @param factory builds the instance the first time `id` is requested
    /// @param <T>     the shared instance type
    /// @return the single shared instance for `id`
    @SuppressWarnings("unchecked")
    public <T> T computeIfAbsent(ComponentId id, Supplier<? extends T> factory) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(factory, "factory");
        var existing = instances.get(id);
        if (existing != null) {
            return (T) existing;
        }
        T created = factory.get();
        instances.put(id, created);
        return created;
    }
}
