package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.Component;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.NetworkBound;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.TelemetryInput;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.FlushReport;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.service.config.ServiceOptions;
import dev.nthings.otlp4j.service.internal.ApplicationInput;
import dev.nthings.otlp4j.service.internal.graph.ServiceGraph;
import java.time.Duration;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/// Owns one validated component graph. Start runs downstream first; shutdown runs upstream first.
/// Every lifecycle operation has one total monotonic budget, including synchronous component code.
/// A runtime starts once and shutdown is idempotent. Failures are observable from flush and close.
/// If component code outlives its deadline, cleanup continues best-effort after that code yields;
/// the caller still observes the timeout. Lifecycle invocations never overlap on that component.
public final class GatewayRuntime implements AutoCloseable {
    private enum State { NEW, STARTING, RUNNING, CLOSED }
    private final ServiceGraph graph;
    private final ComponentContext base;
    private final ServiceOptions options;
    private final List<ComponentRef> dataPathStartOrder;
    private final Map<ComponentKey, Component> byKey;
    private final Map<Component, Semaphore> lifecycleGates = new IdentityHashMap<>();
    private final ReentrantLock operations = new ReentrantLock();
    private volatile State state = State.NEW;
    private volatile boolean closing;
    private CompletionStage<Void> shutdown;
    private FlushReport deferredReport = FlushReport.empty();
    // At most one outstanding flush per component. A caller timeout never discards its report.
    private final Map<Component, CompletableFuture<FlushReport>> pendingFlushes = new IdentityHashMap<>();

    GatewayRuntime(ServiceGraph graph, ComponentContext base, ServiceOptions options,
                   List<ComponentRef> dataPathStartOrder) {
        this.graph = graph;
        this.base = base;
        this.options = options;
        // An actual shared component owns its lifecycle once, even when several nodes reference it.
        var seen = new IdentityHashMap<Component, Boolean>();
        var unique = new ArrayList<ComponentRef>();
        var keyed = new LinkedHashMap<ComponentKey, Component>();
        for (var ref : dataPathStartOrder) {
            keyed.putIfAbsent(ref.key(), ref.component());
            if (seen.put(ref.component(), Boolean.TRUE) == null) {
                unique.add(ref);
                lifecycleGates.put(ref.component(), new Semaphore(1));
            }
        }
        this.dataPathStartOrder = List.copyOf(unique);
        this.byKey = Map.copyOf(keyed);
    }

    public void start() {
        operations.lock();
        try {
            if (state != State.NEW || closing) {
                throw new IllegalStateException("gateway cannot start in state " + state);
            }
            state = State.STARTING;
            var budget = new Budget(options.startDeadline());
            try {
                for (var ref : dataPathStartOrder) {
                    if (closing) { throw new IllegalStateException("shutdown requested during start"); }
                    await(ref, () -> ref.component().start(base), budget, "start");
                }
                if (closing) { throw new IllegalStateException("shutdown requested during start"); }
                state = State.RUNNING;
            } catch (Error | RuntimeException failure) {
                // Factories may have acquired resources before start, and the failing start may
                // have partially initialized its component. Release every created component.
                rollback(failure);
                state = State.CLOSED;
                throw failure;
            }
        } finally {
            operations.unlock();
        }
    }

    private void rollback(Throwable primary) {
        var budget = new Budget(options.shutdownDeadline());
        for (var ref : dataPathStartOrder.reversed()) {
            try {
                await(ref, () -> ref.component().shutdown(new ShutdownContext(budget.remaining())), budget, "rollback");
            } catch (Error | RuntimeException failure) {
                if (failure != primary) { primary.addSuppressed(failure); }
            }
        }
    }

    /// Drains prior input calls, then captures downstream buffer barriers in data-flow order.
    /// Concurrent submissions may be included before a downstream barrier is captured, but cannot
    /// extend that barrier afterward. Reports losses following buffered acceptance.
    public CompletionStage<FlushReport> flush(Duration timeout) {
        var budget = new Budget(timeout);
        var inputBarriers = new IdentityHashMap<Component, CompletionStage<FlushReport>>();
        for (var ref : dataPathStartOrder) {
            if (ref.component() instanceof ApplicationInput<?> source) {
                inputBarriers.put(source, source.flush(new FlushContext(budget.remaining())));
            }
        }
        return asynchronous(budget, false, () -> {
            requireRunning();
            var report = deferredReport;
            Throwable first = null;
            for (var ref : dataPathStartOrder.reversed()) {
                try {
                    if (inputBarriers.containsKey(ref.component())) {
                        // Input barriers contain no loss report and do not cancel submissions.
                        await(ref, () -> inputBarriers.get(ref.component()), budget, "flush");
                        continue;
                    }
                    var previous = pendingFlushes.get(ref.component());
                    if (previous != null) {
                        report = report.plus(awaitFlush(ref, previous, budget));
                    }
                    var current = invokeFlush(ref,
                            () -> ref.component().flush(new FlushContext(budget.remaining())));
                    report = report.plus(awaitFlush(ref, current, budget));
                } catch (Error | RuntimeException failure) {
                    first = accumulate(first, failure);
                    if (budget.remainingNanos() == 0) { break; }
                }
            }
            deferredReport = first == null ? FlushReport.empty() : report;
            rethrow(first);
            return report;
        });
    }

    /// Requests shutdown immediately. Repeated calls observe the original operation and its result.
    public synchronized CompletionStage<Void> shutdown(Duration timeout) {
        if (shutdown != null) { return shutdown; }
        var budget = new Budget(timeout);
        closing = true;
        shutdown = asynchronous(budget, true, () -> {
            if (state == State.CLOSED) { return null; }
            Throwable first = deferredReport.isSuccess() ? null
                    : new IllegalStateException("unreported delivery loss: " + deferredReport, deferredReport.firstFailure());
            for (var ref : dataPathStartOrder.reversed()) {
                try {
                    var pending = pendingFlushes.get(ref.component());
                    if (pending != null) {
                        var report = awaitFlush(ref, pending, budget);
                        if (!report.isSuccess()) {
                            first = accumulate(first, new IllegalStateException(
                                    "unreported delivery loss: " + report, report.firstFailure()));
                        }
                    }
                } catch (Error | RuntimeException failure) {
                    first = accumulate(first, failure);
                }
                try {
                    await(ref, () -> ref.component().shutdown(new ShutdownContext(budget.remaining())), budget, "shutdown");
                } catch (Error | RuntimeException failure) {
                    first = accumulate(first, failure);
                }
            }
            state = State.CLOSED;
            rethrow(first);
            return null;
        });
        return shutdown;
    }

    /// Resolves a typed application input belonging to this graph definition.
    @SuppressWarnings("unchecked")
    public <T> TelemetryInput<T> input(InputHandle<T> handle) {
        Objects.requireNonNull(handle, "handle");
        var component = byKey.get(new ComponentKey(ComponentKind.RECEIVER, handle.id()));
        if (!(component instanceof ApplicationInput<?> candidate) || !candidate.belongsTo(handle.owner())) {
            throw new IllegalArgumentException("input handle does not belong to this gateway");
        }
        var source = (ApplicationInput<T>) candidate;
        return new TelemetryInput<>() {
            @Override public SignalDescriptor<T> signal() { return handle.signal(); }
            @Override public CompletionStage<DeliveryResult> submit(T batch) {
                return submit(batch, ConsumeContext.defaults());
            }
            @Override public CompletionStage<DeliveryResult> submit(T batch, ConsumeContext context) {
                requireRunning();
                return source.submit(batch, context);
            }
            @Override public CompletionStage<FlushReport> flush(Duration timeout) {
                return GatewayRuntime.this.flush(timeout);
            }
        };
    }

    private void requireRunning() {
        if (state != State.RUNNING || closing) { throw new IllegalStateException("gateway is not running"); }
    }

    /// The live component built under `kind` and `id`, if any. This is the unambiguous lookup: it
    /// resolves a component even when another component of a different kind shares the same id (a
    /// Collector-style `otlp` receiver alongside an `otlp` exporter, for example). A multi-signal
    /// exporter resolves to one of its per-signal facets.
    /// This is an advanced inspection escape hatch. Components remain owned by the runtime: callers
    /// must not invoke their lifecycle methods or close them independently.
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


    public String describe() { return graph.describe(); }

    @Override
    public void close() {
        shutdown(options.shutdownDeadline()).toCompletableFuture().join();
    }

    private <T> CompletionStage<T> asynchronous(Budget budget, boolean cleanup, Supplier<T> operation) {
        var result = new CompletableFuture<T>();
        Thread.ofVirtual().name("otlp-gateway-lifecycle").start(() -> {
            boolean locked = false;
            try {
                if (cleanup) { operations.lock(); locked = true; }
                else { locked = operations.tryLock(budget.remainingNanos(), TimeUnit.NANOSECONDS); }
                if (!locked) { throw new TimeoutException("gateway lifecycle deadline expired waiting for another operation"); }
                result.complete(operation.get());
            }
            catch (Throwable failure) { result.completeExceptionally(failure); }
            finally { if (locked) { operations.unlock(); } }
        });
        if (cleanup) { result.orTimeout(budget.remainingNanos(), TimeUnit.NANOSECONDS); }
        return result.minimalCompletionStage();
    }

    private <T> T await(ComponentRef ref, Supplier<CompletionStage<T>> action, Budget budget, String phase) {
        var result = new CompletableFuture<T>();
        var pending = new AtomicReference<CompletableFuture<T>>();
        var gate = lifecycleGates.get(ref.component());
        boolean cleanup = phase.equals("shutdown") || phase.equals("rollback");
        var invocation = Thread.ofVirtual().name("otlp-component-" + phase).start(() -> {
            boolean acquired = false;
            try {
                gate.acquire();
                acquired = true;
                if (result.isCancelled()) {
                    gate.release();
                    return;
                }
                var stage = Objects.requireNonNull(action.get(), "component returned a null stage").toCompletableFuture();
                pending.set(stage);
                stage.whenComplete((value, failure) -> {
                    gate.release();
                    if (failure != null) { result.completeExceptionally(failure); }
                    else { result.complete(value); }
                });
                acquired = false;
                if (result.isCancelled()) { stage.cancel(true); }
            } catch (Throwable failure) {
                if (acquired) { gate.release(); }
                result.completeExceptionally(failure);
            }
        });
        try {
            return result.get(budget.remainingNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException timeout) {
            if (!cleanup) { cancel(result, pending, invocation); }
            throw new GatewayLifecycleException(ref.id(), phase + " exceeded the gateway deadline", timeout);
        } catch (InterruptedException interrupted) {
            if (!cleanup) { cancel(result, pending, invocation); }
            Thread.currentThread().interrupt();
            throw new GatewayLifecycleException(ref.id(), phase + " was interrupted", interrupted);
        } catch (ExecutionException execution) {
            var cause = execution.getCause();
            if (cause instanceof Error error) { throw error; }
            throw new GatewayLifecycleException(ref.id(), phase + " of " + ref.id() + " failed", cause);
        }
    }

    private CompletableFuture<FlushReport> invokeFlush(
            ComponentRef ref, Supplier<CompletionStage<FlushReport>> action) {
        var result = new CompletableFuture<FlushReport>();
        pendingFlushes.put(ref.component(), result);
        var gate = lifecycleGates.get(ref.component());
        Thread.ofVirtual().name("otlp-component-flush").start(() -> {
            boolean acquired = false;
            try {
                gate.acquire();
                acquired = true;
                Objects.requireNonNull(action.get(), "component returned a null flush stage")
                        .toCompletableFuture().whenComplete((report, failure) -> {
                            gate.release();
                            if (failure != null) { result.completeExceptionally(failure); }
                            else if (report == null) {
                                result.completeExceptionally(new NullPointerException("component returned a null flush report"));
                            } else { result.complete(report); }
                        });
                acquired = false;
            } catch (Throwable failure) {
                if (acquired) { gate.release(); }
                result.completeExceptionally(failure);
            }
        });
        return result;
    }

    private FlushReport awaitFlush(ComponentRef ref, CompletableFuture<FlushReport> result, Budget budget) {
        try {
            var report = result.get(budget.remainingNanos(), TimeUnit.NANOSECONDS);
            pendingFlushes.remove(ref.component());
            return report;
        } catch (TimeoutException timeout) {
            throw new GatewayLifecycleException(ref.id(), "flush exceeded the gateway deadline", timeout);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new GatewayLifecycleException(ref.id(), "flush was interrupted", interrupted);
        } catch (ExecutionException execution) {
            pendingFlushes.remove(ref.component());
            var cause = execution.getCause();
            if (cause instanceof Error error) { throw error; }
            throw new GatewayLifecycleException(ref.id(), "flush of " + ref.id() + " failed", cause);
        }
    }

    private static <T> void cancel(CompletableFuture<T> result,
                                   AtomicReference<CompletableFuture<T>> pending, Thread invocation) {
        result.cancel(true);
        var stage = pending.get();
        if (stage != null) { stage.cancel(true); }
        invocation.interrupt();
    }

    private static Throwable accumulate(Throwable first, Throwable failure) {
        if (first == null) { return failure; }
        if (first != failure) { first.addSuppressed(failure); }
        return first;
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof Error error) { throw error; }
        if (failure instanceof RuntimeException runtime) { throw runtime; }
        if (failure != null) { throw new IllegalStateException("gateway lifecycle failed", failure); }
    }

    private static final class Budget {
        private final long started = System.nanoTime();
        private final long timeoutNanos;
        Budget(Duration timeout) {
            Objects.requireNonNull(timeout, "timeout");
            if (timeout.isNegative()) { throw new IllegalArgumentException("timeout must be non-negative"); }
            timeoutNanos = TimeUnit.NANOSECONDS.convert(timeout);
        }
        long remainingNanos() { return Math.max(0, timeoutNanos - (System.nanoTime() - started)); }
        Duration remaining() { return Duration.ofNanos(remainingNanos()); }
    }
}
