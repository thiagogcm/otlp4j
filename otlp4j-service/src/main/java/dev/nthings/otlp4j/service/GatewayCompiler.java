package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.Component;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.Connector;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.SharedComponents;
import dev.nthings.otlp4j.core.component.SignalSupport;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.SignalPair;
import dev.nthings.otlp4j.service.config.ConnectorEntry;
import dev.nthings.otlp4j.service.config.ExporterEntry;
import dev.nthings.otlp4j.service.config.GatewayConfig;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import dev.nthings.otlp4j.service.config.ProcessorEntry;
import dev.nthings.otlp4j.service.config.ReceiverEntry;
import dev.nthings.otlp4j.service.internal.FanOutConsumer;
import dev.nthings.otlp4j.service.internal.GuardedConsumer;
import dev.nthings.otlp4j.service.internal.RoutedConsumers;
import dev.nthings.otlp4j.service.internal.graph.ConnectorNode;
import dev.nthings.otlp4j.service.internal.graph.CycleException;
import dev.nthings.otlp4j.service.internal.graph.Edge;
import dev.nthings.otlp4j.service.internal.graph.ExporterNode;
import dev.nthings.otlp4j.service.internal.graph.FanOutNode;
import dev.nthings.otlp4j.service.internal.graph.Node;
import dev.nthings.otlp4j.service.internal.graph.ProcessorNode;
import dev.nthings.otlp4j.service.internal.graph.ReceiverNode;
import dev.nthings.otlp4j.service.internal.graph.ServiceGraph;
import dev.nthings.otlp4j.service.validation.Diagnostic;
import dev.nthings.otlp4j.service.validation.DiagnosticCode;
import dev.nthings.otlp4j.service.validation.GraphValidationException;
import dev.nthings.otlp4j.core.telemetry.GatewayMetrics;
import java.time.Clock;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import org.jspecify.annotations.Nullable;

/// Turns a validated [GatewayConfig] into an unstarted [GatewayRuntime]. It runs the whole ten-step
/// construction order: it resolves every referenced component, validates existence, signal support,
/// connector pairing, and pipeline shape, materializes the internal service graph (catching a
/// connector cycle and converting it to a diagnostic), detects orphaned components, and only then,
/// with no diagnostic outstanding, instantiates the components in reverse topological order and wires
/// them. Validation always runs to completion and throws a [GraphValidationException] carrying every
/// finding before any component is constructed, so a rejected configuration never leaves a half-built
/// gateway behind. The returned runtime is not yet started; the caller starts it.
///
/// This is the single materialization path both construction forms reach: the fluent
/// [GatewayBuilder] compiles its handles to a config and hands it here, and the declarative
/// [Gateway#compile] passes a directly-built config here.
final class GatewayCompiler {

    private final GatewayConfig config;

    /// Builds a compiler over `config`.
    ///
    /// @param config the gateway configuration to validate and materialize
    GatewayCompiler(GatewayConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config must be non-null");
        }
        this.config = config;
    }

    /// Validates and materializes the gateway with a default component context (no-op metrics and the
    /// system UTC clock).
    ///
    /// @return the unstarted runtime
    GatewayRuntime build() {
        return build(new ComponentContext(GatewayMetrics.noop(), Clock.systemUTC()));
    }

    /// Validates and materializes the gateway with the given base component context.
    ///
    /// @param base the base component context every component is created and started with
    /// @return the unstarted runtime
    GatewayRuntime build(ComponentContext base) {
        if (base == null) {
            throw new IllegalArgumentException("base component context must be non-null");
        }
        var diagnostics = new ArrayList<Diagnostic>();
        var bridgeIds = bridgeIds();
        validateStructure(diagnostics, bridgeIds);
        if (hasUnknownReference(diagnostics)) {
            throw new GraphValidationException(diagnostics);
        }
        ServiceGraph graph;
        try {
            graph = materializeGraph();
        } catch (CycleException cycle) {
            diagnostics.add(new Diagnostic(
                    DiagnosticCode.CYCLE_DETECTED,
                    cycle.getMessage(),
                    cycleComponent(cycle),
                    firstPipeline()));
            throw new GraphValidationException(diagnostics);
        }
        detectOrphans(graph, diagnostics);
        if (!diagnostics.isEmpty()) {
            throw new GraphValidationException(diagnostics);
        }
        return instantiate(graph, base);
    }

    // ---- structural validation -------------------------------------------------------------------

    private void validateStructure(List<Diagnostic> diagnostics, Set<ComponentId> bridgeIds) {
        var reportedBridge = new HashSet<ComponentId>();
        for (var entry : config.pipelines().entrySet()) {
            validatePipeline(entry.getKey(), entry.getValue(), diagnostics, bridgeIds, reportedBridge);
        }
        validateConnectors(diagnostics);
    }

    private void validatePipeline(
            PipelineId pid,
            PipelineConfig pc,
            List<Diagnostic> diagnostics,
            Set<ComponentId> bridgeIds,
            Set<ComponentId> reportedBridge) {
        var signal = pid.signal();
        if (pc.receivers().isEmpty() && pc.processors().isEmpty() && pc.exporters().isEmpty()) {
            diagnostics.add(Diagnostic.ofPipeline(DiagnosticCode.EMPTY_PIPELINE, "pipeline is empty", pid));
            return;
        }
        if (pc.receivers().isEmpty()) {
            diagnostics.add(Diagnostic.ofPipeline(
                    DiagnosticCode.PIPELINE_NO_RECEIVERS, "pipeline has no receivers", pid));
        }
        validateRefs(pc.receivers(), "receiver", this::hasReceiver, this::receiverSupports,
                DiagnosticCode.RECEIVER_SIGNAL_UNSUPPORTED, DiagnosticCode.UNKNOWN_RECEIVER_REF,
                pid, signal, bridgeIds, reportedBridge, diagnostics);
        if (pc.exporters().isEmpty()) {
            diagnostics.add(Diagnostic.ofPipeline(
                    DiagnosticCode.PIPELINE_NO_EXPORTERS, "pipeline has no exporters", pid));
        }
        validateRefs(pc.exporters(), "exporter", this::hasExporter, this::exporterSupports,
                DiagnosticCode.EXPORTER_SIGNAL_UNSUPPORTED, DiagnosticCode.UNKNOWN_EXPORTER_REF,
                pid, signal, bridgeIds, reportedBridge, diagnostics);
        var seen = new HashSet<ComponentId>();
        for (var id : pc.processors()) {
            if (!seen.add(id)) {
                diagnostics.add(Diagnostic.of(
                        DiagnosticCode.DUPLICATE_PROCESSOR_IN_PIPELINE,
                        "processor " + id + " appears more than once in this pipeline",
                        id,
                        pid));
            }
            if (hasProcessor(id)) {
                if (!processorSupports(id, signal)) {
                    diagnostics.add(Diagnostic.of(
                            DiagnosticCode.PROCESSOR_SIGNAL_UNSUPPORTED,
                            "processor " + id + " does not support " + signal,
                            id,
                            pid));
                }
            } else {
                diagnostics.add(Diagnostic.of(
                        DiagnosticCode.UNKNOWN_PROCESSOR_REF, "no processor registered under " + id, id, pid));
            }
        }
    }

    /// Validates one pipeline's receiver or exporter reference list: a registered component of the
    /// expected `role` must support the pipeline's `signal`; an id that instead names a connector is
    /// deferred to pair validation; a bridge id with no registered connector is reported once; anything
    /// else is an unknown reference. The receiver and exporter passes differ only in the `role` word,
    /// the two lookups, and the two diagnostic codes, so both share this body.
    private void validateRefs(
            Iterable<ComponentId> ids,
            String role,
            Predicate<ComponentId> has,
            BiPredicate<ComponentId, SignalDescriptor<?>> supports,
            DiagnosticCode signalUnsupported,
            DiagnosticCode unknownRef,
            PipelineId pid,
            SignalDescriptor<?> signal,
            Set<ComponentId> bridgeIds,
            Set<ComponentId> reportedBridge,
            List<Diagnostic> diagnostics) {
        for (var id : ids) {
            if (has.test(id)) {
                if (!supports.test(id, signal)) {
                    diagnostics.add(Diagnostic.of(
                            signalUnsupported, role + " " + id + " does not support " + signal, id, pid));
                }
            } else if (hasConnector(id)) {
                // A connector routed as this pipeline's receiver/exporter; the pair is validated separately.
            } else if (bridgeIds.contains(id)) {
                if (reportedBridge.add(id)) {
                    diagnostics.add(Diagnostic.ofComponent(
                            DiagnosticCode.UNKNOWN_CONNECTOR_REF,
                            id + " is used as a connector (exporter in one pipeline, receiver in another) "
                                    + "but no connector is registered under it",
                            id));
                }
            } else {
                diagnostics.add(Diagnostic.of(
                        unknownRef, "no " + role + " registered under " + id, id, pid));
            }
        }
    }

    private void validateConnectors(List<Diagnostic> diagnostics) {
        for (var connId : config.connectors().keySet()) {
            var exporting = pipelinesUsing(connId, true);
            var receiving = pipelinesUsing(connId, false);
            var usedAsExporter = !exporting.isEmpty();
            var usedAsReceiver = !receiving.isEmpty();
            if (usedAsExporter && !usedAsReceiver) {
                diagnostics.add(Diagnostic.ofComponent(
                        DiagnosticCode.CONNECTOR_USED_AS_EXPORTER_ONLY,
                        "connector " + connId + " is routed as an exporter but never as a receiver",
                        connId));
            } else if (usedAsReceiver && !usedAsExporter) {
                diagnostics.add(Diagnostic.ofComponent(
                        DiagnosticCode.CONNECTOR_USED_AS_RECEIVER_ONLY,
                        "connector " + connId + " is routed as a receiver but never as an exporter",
                        connId));
            } else if (usedAsExporter) {
                var entry = connector(connId);
                for (var exp : exporting) {
                    for (var recv : receiving) {
                        if (!supportsPair(entry, exp.signal(), recv.signal())) {
                            diagnostics.add(Diagnostic.ofComponent(
                                    DiagnosticCode.CONNECTOR_PAIR_UNSUPPORTED,
                                    "connector " + connId + " does not support the pair " + exp.signal()
                                            + " -> " + recv.signal(),
                                    connId));
                        }
                    }
                }
                var inputSignals = distinctSignals(exporting);
                var outputSignals = distinctSignals(receiving);
                if (inputSignals.size() > 1 || outputSignals.size() > 1) {
                    diagnostics.add(Diagnostic.ofComponent(
                            DiagnosticCode.CONNECTOR_MULTIPLE_DISTINCT_PAIRS,
                            "connector " + connId + " bridges more than one distinct signal pair (inputs "
                                    + inputSignals + ", outputs " + outputSignals + "); the graph represents a "
                                    + "connector as a single node carrying one signal pair",
                            connId));
                }
            }
        }
    }

    // ---- graph materialization -------------------------------------------------------------------

    private ServiceGraph materializeGraph() {
        var graph = new ServiceGraph();
        var connectorPairs = connectorPairs();
        for (var entry : config.pipelines().entrySet()) {
            addPipelineEdges(graph, entry.getKey(), entry.getValue(), connectorPairs);
        }
        insertFanOuts(graph);
        return graph;
    }

    private void addPipelineEdges(
            ServiceGraph graph,
            PipelineId pid,
            PipelineConfig pc,
            Map<ComponentId, SignalPair<?, ?>> connectorPairs) {
        var signal = pid.signal();
        var sources = new ArrayList<Node>();
        for (var id : pc.receivers()) {
            if (hasReceiver(id)) {
                sources.add(node(graph, new ReceiverNode(id)));
            } else if (connectorPairs.containsKey(id)) {
                sources.add(node(graph, new ConnectorNode(id, connectorPairs.get(id))));
            }
        }
        var sinks = new ArrayList<Node>();
        for (var id : pc.exporters()) {
            if (hasExporter(id)) {
                sinks.add(node(graph, new ExporterNode(id, signal)));
            } else if (connectorPairs.containsKey(id)) {
                sinks.add(node(graph, new ConnectorNode(id, connectorPairs.get(id))));
            }
        }
        var processors = new ArrayList<Node>();
        for (var id : pc.processors()) {
            var processorNode = new ProcessorNode(id, pid, signal);
            // A duplicate processor id in one pipeline is a structural diagnostic; here we dedupe the
            // chain node so it does not become a spurious self-loop and confuse cycle detection.
            if (!processors.contains(processorNode)) {
                processors.add(node(graph, processorNode));
            }
        }
        if (processors.isEmpty()) {
            for (var src : sources) {
                for (var sink : sinks) {
                    graph.addEdge(src, sink, new Edge(signal));
                }
            }
            return;
        }
        for (var src : sources) {
            graph.addEdge(src, processors.get(0), new Edge(signal));
        }
        for (var i = 0; i < processors.size() - 1; i++) {
            graph.addEdge(processors.get(i), processors.get(i + 1), new Edge(signal));
        }
        var tail = processors.get(processors.size() - 1);
        for (var sink : sinks) {
            graph.addEdge(tail, sink, new Edge(signal));
        }
    }

    private static Node node(ServiceGraph graph, Node node) {
        graph.addNode(node);
        return node;
    }

    private void insertFanOuts(ServiceGraph graph) {
        var sequence = 0;
        for (var producer : new ArrayList<>(graph.nodes())) {
            var bySignal = new LinkedHashMap<SignalDescriptor<?>, List<Edge>>();
            for (var edge : new ArrayList<>(graph.outgoing(producer))) {
                bySignal.computeIfAbsent(edge.signal(), s -> new ArrayList<>()).add(edge);
            }
            for (var group : bySignal.entrySet()) {
                if (group.getValue().size() <= 1) {
                    continue;
                }
                var signal = group.getKey();
                var fanOut = new FanOutNode(signal, sequence++);
                graph.addNode(fanOut);
                for (var edge : group.getValue()) {
                    var target = graph.target(edge);
                    graph.removeEdge(edge);
                    graph.addEdge(fanOut, target, new Edge(signal));
                }
                graph.addEdge(producer, fanOut, new Edge(signal));
            }
        }
    }

    // ---- orphan detection ------------------------------------------------------------------------

    private void detectOrphans(ServiceGraph graph, List<Diagnostic> diagnostics) {
        var forward = new HashSet<Node>();
        var backward = new HashSet<Node>();
        for (var n : graph.nodes()) {
            if (n instanceof ReceiverNode) {
                forward.add(n);
                forward.addAll(graph.descendants(n));
            }
            if (n instanceof ExporterNode) {
                backward.add(n);
                backward.addAll(graph.ancestors(n));
            }
        }
        for (var n : graph.nodes()) {
            if (n instanceof FanOutNode) {
                continue;
            }
            if (!(forward.contains(n) && backward.contains(n))) {
                componentId(n).ifPresent(id -> diagnostics.add(Diagnostic.ofComponent(
                        DiagnosticCode.ORPHANED_COMPONENT,
                        n.label() + " is not on any path from a receiver to an exporter",
                        id)));
            }
        }
        for (var id : unreferencedComponents()) {
            diagnostics.add(Diagnostic.ofComponent(
                    DiagnosticCode.ORPHANED_COMPONENT, "component " + id + " is configured but never referenced", id));
        }
    }

    private Set<ComponentId> unreferencedComponents() {
        var referenced = new HashSet<ComponentId>();
        for (var pc : config.pipelines().values()) {
            referenced.addAll(pc.receivers());
            referenced.addAll(pc.processors());
            referenced.addAll(pc.exporters());
        }
        var orphans = new ArrayList<ComponentId>();
        collectUnreferenced(config.receivers().keySet(), referenced, orphans);
        collectUnreferenced(config.processors().keySet(), referenced, orphans);
        collectUnreferenced(config.exporters().keySet(), referenced, orphans);
        collectUnreferenced(config.connectors().keySet(), referenced, orphans);
        return new HashSet<>(orphans);
    }

    private static void collectUnreferenced(
            Set<ComponentId> registered, Set<ComponentId> referenced, List<ComponentId> orphans) {
        for (var id : registered) {
            if (!referenced.contains(id)) {
                orphans.add(id);
            }
        }
    }

    // ---- instantiation ---------------------------------------------------------------------------

    private GatewayRuntime instantiate(ServiceGraph graph, ComponentContext base) {
        var order = graph.topoOrder();
        var inConsumer = new HashMap<Node, SignalConsumer<?>>();
        var dataPathStartOrder = new ArrayList<ComponentRef>();
        // Share across signals, but never across component kinds: receiver and exporter ids may match.
        var shared = new EnumMap<ComponentKind, SharedComponents>(ComponentKind.class);
        try {
            for (var node : order.reverse()) {
                switch (node) {
                    case ExporterNode exporter -> {
                        var ctx = new CreateContext(exporter.id(), base,
                                shared.computeIfAbsent(ComponentKind.EXPORTER, _ -> new SharedComponents()));
                        var provenance = new Provenance(exporter.id(), ComponentKind.EXPORTER, exporter.signal());
                        var built = buildExporter(exporter(exporter.id()), ctx, exporter.signal(), provenance);
                        inConsumer.put(node, built.guarded());
                        dataPathStartOrder.add(
                                new ComponentRef(ComponentKind.EXPORTER, exporter.id(), built.component()));
                    }
                    case ProcessorNode processor -> {
                        var next = singleDownstream(graph, node, inConsumer);
                        var ctx = new CreateContext(processor.id(), base,
                                shared.computeIfAbsent(ComponentKind.PROCESSOR, _ -> new SharedComponents()));
                        var provenance = new Provenance(processor.id(), ComponentKind.PROCESSOR, processor.signal());
                        var built = buildProcessor(
                                processor(processor.id()), ctx, processor.signal(), next, provenance);
                        inConsumer.put(node, built.guarded());
                        dataPathStartOrder.add(
                                new ComponentRef(ComponentKind.PROCESSOR, processor.id(), built.component()));
                    }
                    case ConnectorNode connector -> {
                        var next = singleDownstream(graph, node, inConsumer);
                        var ctx = new CreateContext(connector.id(), base,
                                shared.computeIfAbsent(ComponentKind.CONNECTOR, _ -> new SharedComponents()));
                        var built = buildConnector(connector(connector.id()), ctx, connector.pair(), next);
                        inConsumer.put(node, built.guarded());
                        dataPathStartOrder.add(
                                new ComponentRef(ComponentKind.CONNECTOR, connector.id(), built.component()));
                    }
                    case FanOutNode fanOut -> inConsumer.put(node, fanOut(downstreams(graph, node, inConsumer)));
                    case ReceiverNode receiver -> {
                        var bundle = new LinkedHashMap<SignalDescriptor<?>, SignalConsumer<?>>();
                        for (var edge : graph.outgoing(node)) {
                            bundle.put(edge.signal(), inConsumer.get(graph.target(edge)));
                        }
                        var ctx = new CreateContext(receiver.id(), base,
                                shared.computeIfAbsent(ComponentKind.RECEIVER, _ -> new SharedComponents()));
                        var built = receiver(receiver.id()).create(ctx, new RoutedConsumers(bundle));
                        dataPathStartOrder.add(new ComponentRef(ComponentKind.RECEIVER, receiver.id(), built));
                    }
                }
            }
        } catch (Error | RuntimeException failure) {
            try {
                new GatewayRuntime(graph, base, config.service(), dataPathStartOrder)
                        .shutdown(config.service().shutdownDeadline()).toCompletableFuture().join();
            } catch (Error | RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
        return new GatewayRuntime(graph, base, config.service(), dataPathStartOrder);
    }

    private SignalConsumer<?> singleDownstream(
            ServiceGraph graph, Node node, Map<Node, SignalConsumer<?>> inConsumer) {
        var out = graph.outgoing(node);
        if (out.size() != 1) {
            throw new IllegalStateException(
                    "expected exactly one downstream for " + node.label() + " after fan-out insertion, got "
                            + out.size());
        }
        var edge = out.iterator().next();
        return inConsumer.get(graph.target(edge));
    }

    private List<SignalConsumer<?>> downstreams(
            ServiceGraph graph, Node node, Map<Node, SignalConsumer<?>> inConsumer) {
        var peers = new ArrayList<SignalConsumer<?>>();
        for (var edge : graph.outgoing(node)) {
            peers.add(inConsumer.get(graph.target(edge)));
        }
        return peers;
    }

    // ---- typed instantiation helpers -------------------------------------------------------------

    private record Built(Component component, SignalConsumer<?> guarded) {}

    private static <T> Built buildExporter(
            ExporterEntry<?> entry, CreateContext ctx, SignalDescriptor<?> signal, Provenance provenance) {
        SignalDescriptor<T> typed = castSignal(signal);
        Exporter<T> exporter = entry.create(ctx, typed);
        return new Built(exporter, new GuardedConsumer<>(exporter, provenance));
    }

    private static <T> Built buildProcessor(
            ProcessorEntry<?> entry,
            CreateContext ctx,
            SignalDescriptor<?> signal,
            SignalConsumer<?> next,
            Provenance provenance) {
        var typedSignal = GatewayCompiler.<T>castSignal(signal);
        SignalConsumer<T> typedNext = castConsumer(next);
        Processor<T> processor = entry.create(ctx, typedSignal, typedNext);
        return new Built(processor, new GuardedConsumer<>(processor, provenance));
    }

    private static Built buildConnector(
            ConnectorEntry<?> entry, CreateContext ctx, SignalPair<?, ?> pair, SignalConsumer<?> next) {
        Connector connector = entry.create(ctx, pair, next);
        if (!(connector instanceof SignalConsumer<?>)) {
            throw new IllegalStateException(
                    "connector " + ctx.id() + " must also implement SignalConsumer for its input signal " + pair.from());
        }
        var provenance = new Provenance(ctx.id(), ComponentKind.CONNECTOR, pair.from());
        SignalConsumer<?> asConsumer = (SignalConsumer<?>) connector;
        return new Built(connector, guard(asConsumer, provenance));
    }

    private static <T> SignalConsumer<T> fanOut(List<SignalConsumer<?>> peers) {
        var typed = new ArrayList<SignalConsumer<T>>(peers.size());
        for (var peer : peers) {
            typed.add(GatewayCompiler.<T>castConsumer(peer));
        }
        return new FanOutConsumer<>(typed);
    }

    private static <T> SignalConsumer<T> guard(
            SignalConsumer<?> consumer, Provenance provenance) {
        return new GuardedConsumer<>(GatewayCompiler.<T>castConsumer(consumer), provenance);
    }

    @SuppressWarnings("unchecked")
    private static <T> SignalDescriptor<T> castSignal(SignalDescriptor<?> signal) {
        return (SignalDescriptor<T>) signal;
    }

    @SuppressWarnings("unchecked")
    private static <T> SignalConsumer<T> castConsumer(SignalConsumer<?> consumer) {
        return (SignalConsumer<T>) consumer;
    }

    // ---- shared lookups --------------------------------------------------------------------------

    private Set<ComponentId> bridgeIds() {
        var asExporter = new HashSet<ComponentId>();
        var asReceiver = new HashSet<ComponentId>();
        for (var pc : config.pipelines().values()) {
            asExporter.addAll(pc.exporters());
            asReceiver.addAll(pc.receivers());
        }
        var bridges = new HashSet<>(asExporter);
        bridges.retainAll(asReceiver);
        return bridges;
    }

    private Map<ComponentId, SignalPair<?, ?>> connectorPairs() {
        var pairs = new HashMap<ComponentId, SignalPair<?, ?>>();
        for (var connId : config.connectors().keySet()) {
            var exporting = pipelinesUsing(connId, true);
            var receiving = pipelinesUsing(connId, false);
            if (!exporting.isEmpty() && !receiving.isEmpty()) {
                pairs.put(connId, makePair(exporting.get(0).signal(), receiving.get(0).signal()));
            }
        }
        return pairs;
    }

    private static Set<SignalDescriptor<?>> distinctSignals(List<PipelineId> pipelines) {
        var signals = new LinkedHashSet<SignalDescriptor<?>>();
        for (var pid : pipelines) {
            signals.add(pid.signal());
        }
        return signals;
    }

    private List<PipelineId> pipelinesUsing(ComponentId connId, boolean asExporter) {
        var result = new ArrayList<PipelineId>();
        for (var entry : config.pipelines().entrySet()) {
            var ids = asExporter ? entry.getValue().exporters() : entry.getValue().receivers();
            if (ids.contains(connId)) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    private static boolean supportsPair(
            ConnectorEntry<?> entry, SignalDescriptor<?> from, SignalDescriptor<?> to) {
        for (var pair : entry.supportedPairs()) {
            if (pair.from().equals(from) && pair.to().equals(to)) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static <I, O> SignalPair<I, O> makePair(SignalDescriptor<?> from, SignalDescriptor<?> to) {
        return new SignalPair<>((SignalDescriptor<I>) from, (SignalDescriptor<O>) to);
    }

    private static boolean hasUnknownReference(List<Diagnostic> diagnostics) {
        for (var d : diagnostics) {
            switch (d.code()) {
                case UNKNOWN_RECEIVER_REF, UNKNOWN_PROCESSOR_REF, UNKNOWN_EXPORTER_REF, UNKNOWN_CONNECTOR_REF -> {
                    return true;
                }
                default -> {
                    // keep scanning
                }
            }
        }
        return false;
    }

    private Optional<ComponentId> cycleComponent(CycleException cycle) {
        return componentId(cycle.to()).or(() -> componentId(cycle.from()));
    }

    private Optional<PipelineId> firstPipeline() {
        return config.pipelines().keySet().stream().findFirst();
    }

    private static Optional<ComponentId> componentId(Node node) {
        return switch (node) {
            case ReceiverNode r -> Optional.of(r.id());
            case ProcessorNode p -> Optional.of(p.id());
            case ExporterNode e -> Optional.of(e.id());
            case ConnectorNode c -> Optional.of(c.id());
            case FanOutNode ignored -> Optional.empty();
        };
    }

    // ---- config lookups (the folded-in registry) -------------------------------------------------
    //
    // The declarative resolution over the config's five kind maps: existence, entry, and the
    // optional per-factory SignalSupport restriction the validator consults. A component may be
    // referenced by a pipeline in a role its kind does not match (a receiver id in an exporter list),
    // so each kind lives in its own map and a role query is a map lookup; a factory that declares no
    // SignalSupport is treated as supporting every signal.

    private boolean hasReceiver(ComponentId id) {
        return config.receivers().containsKey(id);
    }

    private boolean hasProcessor(ComponentId id) {
        return config.processors().containsKey(id);
    }

    private boolean hasExporter(ComponentId id) {
        return config.exporters().containsKey(id);
    }

    private boolean hasConnector(ComponentId id) {
        return config.connectors().containsKey(id);
    }

    private ReceiverEntry<?> receiver(ComponentId id) {
        return config.receivers().get(id);
    }

    private ProcessorEntry<?> processor(ComponentId id) {
        return config.processors().get(id);
    }

    private ExporterEntry<?> exporter(ComponentId id) {
        return config.exporters().get(id);
    }

    private ConnectorEntry<?> connector(ComponentId id) {
        return config.connectors().get(id);
    }

    private boolean receiverSupports(ComponentId id, SignalDescriptor<?> signal) {
        return supports(config.receivers().get(id), signal);
    }

    private boolean processorSupports(ComponentId id, SignalDescriptor<?> signal) {
        return supports(config.processors().get(id), signal);
    }

    private boolean exporterSupports(ComponentId id, SignalDescriptor<?> signal) {
        return supports(config.exporters().get(id), signal);
    }

    private static boolean supports(@Nullable Object entry, SignalDescriptor<?> signal) {
        Object factory = switch (entry) {
            case ReceiverEntry<?> r -> r.factory();
            case ProcessorEntry<?> p -> p.factory();
            case ExporterEntry<?> e -> e.factory();
            case null, default -> null;
        };
        return !(factory instanceof SignalSupport support) || support.supportsSignal(signal);
    }
}
