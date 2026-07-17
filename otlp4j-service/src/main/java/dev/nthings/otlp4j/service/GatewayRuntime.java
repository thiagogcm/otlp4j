package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.Component;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.NetworkBound;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.service.config.ServiceOptions;
import dev.nthings.otlp4j.service.internal.graph.ServiceGraph;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// The started gateway: the built components plus the graph's topological order, the declared
/// replacement for the fluent pipeline's discovered lifecycle. It owns the components and derives
/// start and shutdown order from the graph, not from registration order.
///
/// [#start()] starts the data-path components in reverse topological order, so a downstream is live
/// before the upstream that feeds it and receivers start last. Each start is bounded by the service
/// start deadline; on any start failure the already-started components are shut down in reverse start
/// order and the failure is rethrown, so a half-started gateway never serves.
/// [#shutdown(Duration)] walks the data path in forward topological order so receivers stop first and
/// in-flight work drains into still-open exporters, best-effort with the first error rethrown at the
/// end. [#component(ComponentKind, ComponentId)] (and its convenience
/// [#component(ComponentId)] overload for an unambiguous id) is the only path from a post-construction
/// runtime to a live component instance, which a caller downcasts to a capability such as
/// [dev.nthings.otlp4j.core.component.NetworkBound] to read, for example, a receiver's bound port.
/// Live components are keyed by the composite (kind, id), so a receiver and an exporter that reuse one
/// id resolve to distinct instances rather than colliding.
public final class GatewayRuntime implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(GatewayRuntime.class);

    private final ServiceGraph graph;

    private final ComponentContext base;

    private final ServiceOptions options;

    /// Data-path components in reverse topological order, which is their start order.
    private final List<ComponentRef> dataPathStartOrder;

    /// Live components keyed by the composite (kind, id), so a receiver and an exporter that share an
    /// id do not collide. A multi-signal exporter, realized as one per-signal facet node per signal,
    /// resolves to the first facet built under its (kind, id) key.
    private final Map<ComponentKey, Component> byKey;

    GatewayRuntime(
            ServiceGraph graph,
            ComponentContext base,
            ServiceOptions options,
            List<ComponentRef> dataPathStartOrder) {
        this.graph = graph;
        this.base = base;
        this.options = options;
        this.dataPathStartOrder = List.copyOf(dataPathStartOrder);
        var keyed = new LinkedHashMap<ComponentKey, Component>();
        for (var ref : this.dataPathStartOrder) {
            keyed.putIfAbsent(ref.key(), ref.component());
        }
        this.byKey = Map.copyOf(keyed);
    }

    /// Starts the gateway: the data-path components in reverse topological order, each bounded by the
    /// start deadline. On any failure the already-started components are shut down in reverse start
    /// order and the failure is rethrown.
    ///
    /// @throws GatewayStartException if a component fails to start, times out, or the thread is
    ///     interrupted
    public void start() {
        var started = new ArrayList<ComponentRef>();
        try {
            for (var ref : dataPathStartOrder) {
                startOne(ref);
                started.add(ref);
            }
        } catch (Error | RuntimeException failure) {
            // await() rethrows an Error cause as an Error and every other start failure as a
            // GatewayStartException, so this covers both: roll back before the failure escapes, so an
            // Error-typed start failure cannot leave a half-started gateway serving.
            rollback(started);
            throw failure;
        }
    }

    private void startOne(ComponentRef ref) {
        await(ref, ref.component().start(base), options.startDeadline(), "start");
    }

    private void rollback(List<ComponentRef> started) {
        var reverse = new ArrayList<>(started);
        Collections.reverse(reverse);
        for (var ref : reverse) {
            try {
                shutdownOne(ref, options.shutdownDeadline());
            } catch (Error | RuntimeException rollbackFailure) {
                // Best-effort: an Error from one component's rollback shutdown must not abort the
                // rollback of the components still started behind it.
                LOG.warn("rollback shutdown of {} failed", ref.id(), rollbackFailure);
            }
        }
    }

    /// Flushes every component best-effort within `deadline`, in forward data-path order.
    ///
    /// @param deadline the per-component flush deadline
    public void forceFlush(Duration deadline) {
        var context = new FlushContext(deadline);
        Throwable first = null;
        for (var ref : forwardDataPath()) {
            first = flushOne(ref, context, first);
        }
        rethrowIfPresent(first, "forceFlush");
    }

    private Throwable flushOne(ComponentRef ref, FlushContext context, Throwable first) {
        try {
            await(ref, ref.component().forceFlush(context), context.deadline(), "forceFlush");
            return first;
        } catch (Error | RuntimeException failure) {
            // Best-effort: accumulate an Error-typed flush failure like any other and keep flushing
            // the remaining components; rethrowIfPresent re-surfaces it (as an Error) at the end.
            LOG.warn("forceFlush of {} failed", ref.id(), failure);
            return first == null ? failure : first;
        }
    }

    /// Shuts the gateway down: the data path in forward topological order so receivers stop first.
    /// Best-effort, with the first error rethrown at the end.
    ///
    /// @param deadline the per-component shutdown deadline
    public void shutdown(Duration deadline) {
        Throwable first = null;
        for (var ref : forwardDataPath()) {
            first = shutdownBestEffort(ref, deadline, first);
        }
        rethrowIfPresent(first, "shutdown");
    }

    private Throwable shutdownBestEffort(ComponentRef ref, Duration deadline, Throwable first) {
        try {
            shutdownOne(ref, deadline);
            return first;
        } catch (Error | RuntimeException failure) {
            // Best-effort: an Error from one component's shutdown must not abort the shutdown of the
            // components behind it; rethrowIfPresent re-surfaces it at the end.
            LOG.warn("shutdown of {} failed", ref.id(), failure);
            return first == null ? failure : first;
        }
    }

    private void shutdownOne(ComponentRef ref, Duration deadline) {
        await(ref, ref.component().shutdown(new ShutdownContext(deadline)), deadline, "shutdown");
    }

    /// The live component built under `kind` and `id`, if any. This is the unambiguous lookup: it
    /// resolves a component even when another component of a different kind shares the same id (a
    /// Collector-style `otlp` receiver alongside an `otlp` exporter, for example). A multi-signal
    /// exporter resolves to one of its per-signal facets.
    ///
    /// @param kind the component kind
    /// @param id   the component id
    /// @return the live component, or empty when no component of that kind was built under that id
    public Optional<Component> component(ComponentKind kind, ComponentId id) {
        return Optional.ofNullable(byKey.get(new ComponentKey(kind, id)));
    }

    /// The live component built under `id`, if any, when that id is unambiguous. A component
    /// instantiated once per id (a receiver, a connector) resolves unambiguously; a multi-signal
    /// exporter resolves to one of its per-signal facets. When two components of different kinds share
    /// the id this method cannot answer without guessing, so it throws and the caller must use
    /// [#component(ComponentKind, ComponentId)] instead, which is always unambiguous.
    ///
    /// @param id the component id
    /// @return the live component, or empty when no component was built under that id
    /// @throws IllegalArgumentException if more than one kind of component was built under that id
    public Optional<Component> component(ComponentId id) {
        Component match = null;
        for (var entry : byKey.entrySet()) {
            if (entry.getKey().id().equals(id)) {
                if (match != null) {
                    throw new IllegalArgumentException("component id " + id
                            + " is registered under more than one kind; use component(ComponentKind, ComponentId)"
                            + " to disambiguate");
                }
                match = entry.getValue();
            }
        }
        return Optional.ofNullable(match);
    }

    /// The bound network port of the component built under `id`, when that component binds a socket
    /// and has started. This is the direct replacement for the four-step `component(id)` to
    /// [NetworkBound] downcast dance: it resolves the component through the unambiguous
    /// [#component(ComponentId)] lookup, narrows it to [NetworkBound], and reads its bound port,
    /// returning empty when no component is registered under `id` or the one that is does not bind a
    /// port. For a receiver bound to an ephemeral port this returns the OS-assigned port after start.
    ///
    /// @param id the component id
    /// @return the bound port, or empty when the component is absent or not network-bound
    public OptionalInt boundPort(ComponentId id) {
        return component(id).orElse(null) instanceof NetworkBound bound
                ? OptionalInt.of(bound.port())
                : OptionalInt.empty();
    }

    /// A DOT-style dump of the service graph, replacing the fluent pipeline's description.
    ///
    /// @return the graph dump
    public String describe() {
        return graph.describe();
    }

    /// Shuts the gateway down within the configured shutdown deadline, swallowing a failure as a WARN
    /// log rather than throwing, so the runtime can be used in a try-with-resources block.
    @Override
    public void close() {
        try {
            shutdown(options.shutdownDeadline());
        } catch (RuntimeException failure) {
            LOG.warn("gateway shutdown failed during close()", failure);
        }
    }

    private List<ComponentRef> forwardDataPath() {
        return reversed(dataPathStartOrder);
    }

    private static List<ComponentRef> reversed(List<ComponentRef> refs) {
        var copy = new ArrayList<>(refs);
        Collections.reverse(copy);
        return copy;
    }

    private void await(ComponentRef ref, CompletionStage<Void> stage, Duration deadline, String phase) {
        try {
            stage.toCompletableFuture().get(deadline.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeout) {
            throw new GatewayStartException(
                    ref.id(), phase + " of " + ref.id() + " timed out after " + deadline, timeout);
        } catch (ExecutionException execution) {
            var cause = execution.getCause() == null ? execution : execution.getCause();
            if (cause instanceof Error error) {
                throw error;
            }
            throw new GatewayStartException(ref.id(), phase + " of " + ref.id() + " failed", cause);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new GatewayStartException(ref.id(), phase + " of " + ref.id() + " was interrupted", interrupted);
        }
    }

    private static void rethrowIfPresent(Throwable first, String phase) {
        if (first == null) {
            return;
        }
        if (first instanceof Error error) {
            throw error;
        }
        if (first instanceof RuntimeException runtime) {
            throw runtime;
        }
        throw new IllegalStateException(phase + " failed", first);
    }
}
