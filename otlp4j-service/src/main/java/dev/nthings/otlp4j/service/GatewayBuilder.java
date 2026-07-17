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
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.pipeline.PipelineId;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.SignalPair;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.service.config.ComponentRegistry;
import dev.nthings.otlp4j.service.config.ConnectorEntry;
import dev.nthings.otlp4j.service.config.ExporterEntry;
import dev.nthings.otlp4j.service.config.GatewayConfig;
import dev.nthings.otlp4j.service.config.PipelineConfig;
import dev.nthings.otlp4j.service.config.ProcessorEntry;
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
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import dev.nthings.otlp4j.telemetry.GatewayMetrics;

/// Turns a validated [GatewayConfig] into an unstarted [GatewayRuntime]. It runs the whole ten-step
/// construction order: it resolves every referenced component, validates existence, signal support,
/// connector pairing, and pipeline shape, materializes the internal service graph (catching a
/// connector cycle and converting it to a diagnostic), detects orphaned components, and only then,
/// with no diagnostic outstanding, instantiates the components in reverse topological order and wires
/// them. Validation always runs to completion and throws a [GraphValidationException] carrying every
/// finding before any component is constructed, so a rejected configuration never leaves a half-built
/// gateway behind. The returned runtime is not yet started; the caller starts it.
public final class GatewayBuilder {

    private final GatewayConfig config;

    private final ComponentRegistry registry;

    /// Builds a builder over `config`.
    ///
    /// @param config the gateway configuration to validate and materialize
    public GatewayBuilder(GatewayConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config must be non-null");
        }
        this.config = config;
        this.registry = new ComponentRegistry(config);
    }

    /// Validates and materializes the gateway with a default component context (no-op metrics and the
    /// system UTC clock).
    ///
    /// @return the unstarted runtime
    public GatewayRuntime build() {
        return build(new ComponentContext(GatewayMetrics.noop(), Clock.systemUTC()));
    }

    /// Validates and materializes the gateway with the given base component context.
    ///
    /// @param base the base component context every component is created and started with
    /// @return the unstarted runtime
    public GatewayRuntime build(ComponentContext base) {
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
        for (var id : pc.receivers()) {
            if (registry.hasReceiver(id)) {
                if (!registry.receiverSupports(id, signal)) {
                    diagnostics.add(Diagnostic.of(
                            DiagnosticCode.RECEIVER_SIGNAL_UNSUPPORTED,
                            "receiver " + id + " does not support " + signal,
                            id,
                            pid));
                }
            } else if (registry.hasConnector(id)) {
                // A connector routed as this pipeline's receiver; the pair is validated separately.
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
                        DiagnosticCode.UNKNOWN_RECEIVER_REF, "no receiver registered under " + id, id, pid));
            }
        }
        if (pc.exporters().isEmpty()) {
            diagnostics.add(Diagnostic.ofPipeline(
                    DiagnosticCode.PIPELINE_NO_EXPORTERS, "pipeline has no exporters", pid));
        }
        for (var id : pc.exporters()) {
            if (registry.hasExporter(id)) {
                if (!registry.exporterSupports(id, signal)) {
                    diagnostics.add(Diagnostic.of(
                            DiagnosticCode.EXPORTER_SIGNAL_UNSUPPORTED,
                            "exporter " + id + " does not support " + signal,
                            id,
                            pid));
                }
            } else if (registry.hasConnector(id)) {
                // A connector routed as this pipeline's exporter; the pair is validated separately.
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
                        DiagnosticCode.UNKNOWN_EXPORTER_REF, "no exporter registered under " + id, id, pid));
            }
        }
        var seen = new HashSet<ComponentId>();
        for (var id : pc.processors()) {
            if (!seen.add(id)) {
                diagnostics.add(Diagnostic.of(
                        DiagnosticCode.DUPLICATE_PROCESSOR_IN_PIPELINE,
                        "processor " + id + " appears more than once in this pipeline",
                        id,
                        pid));
            }
            if (registry.hasProcessor(id)) {
                if (!registry.processorSupports(id, signal)) {
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
                var entry = registry.connector(connId);
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
                                    + inputSignals + ", outputs " + outputSignals + "); Phase 1 represents a "
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
            if (registry.hasReceiver(id)) {
                sources.add(node(graph, new ReceiverNode(id)));
            } else if (connectorPairs.containsKey(id)) {
                sources.add(node(graph, new ConnectorNode(id, connectorPairs.get(id))));
            }
        }
        var sinks = new ArrayList<Node>();
        for (var id : pc.exporters()) {
            if (registry.hasExporter(id)) {
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
        // One shared-component cache for the whole build: an exporter id that terminates several
        // signals is backed by exactly one underlying instance, its per-signal facets sharing it.
        var shared = new SharedComponents();
        for (var node : order.reverse()) {
            switch (node) {
                case ExporterNode exporter -> {
                    var ctx = new CreateContext(exporter.id(), base, shared);
                    var provenance = new Provenance(exporter.id(), ComponentKind.EXPORTER, exporter.signal());
                    var built = buildExporter(registry.exporter(exporter.id()), ctx, exporter.signal(), provenance);
                    inConsumer.put(node, built.guarded());
                    dataPathStartOrder.add(
                            new ComponentRef(ComponentKind.EXPORTER, exporter.id(), built.component()));
                }
                case ProcessorNode processor -> {
                    var next = singleDownstream(graph, node, inConsumer);
                    var ctx = new CreateContext(processor.id(), base, shared);
                    var provenance = new Provenance(processor.id(), ComponentKind.PROCESSOR, processor.signal());
                    var built = buildProcessor(
                            registry.processor(processor.id()), ctx, processor.signal(), next, provenance);
                    inConsumer.put(node, built.guarded());
                    dataPathStartOrder.add(
                            new ComponentRef(ComponentKind.PROCESSOR, processor.id(), built.component()));
                }
                case ConnectorNode connector -> {
                    var next = singleDownstream(graph, node, inConsumer);
                    var ctx = new CreateContext(connector.id(), base, shared);
                    var built = buildConnector(registry.connector(connector.id()), ctx, connector.pair(), next);
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
                    var ctx = new CreateContext(receiver.id(), base, shared);
                    var built = registry.receiver(receiver.id()).create(ctx, new RoutedConsumers(bundle));
                    dataPathStartOrder.add(new ComponentRef(ComponentKind.RECEIVER, receiver.id(), built));
                }
            }
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

    private static <T extends TelemetryPayload> Built buildExporter(
            ExporterEntry<?> entry, CreateContext ctx, SignalDescriptor<?> signal, Provenance provenance) {
        SignalDescriptor<T> typed = castSignal(signal);
        Exporter<T> exporter = entry.create(ctx, typed);
        return new Built(exporter, new GuardedConsumer<>(exporter, provenance));
    }

    private static <T extends TelemetryPayload> Built buildProcessor(
            ProcessorEntry<?> entry,
            CreateContext ctx,
            SignalDescriptor<?> signal,
            SignalConsumer<?> next,
            Provenance provenance) {
        var typedSignal = GatewayBuilder.<T>castSignal(signal);
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

    private static <T extends TelemetryPayload> SignalConsumer<T> fanOut(List<SignalConsumer<?>> peers) {
        var typed = new ArrayList<SignalConsumer<T>>(peers.size());
        for (var peer : peers) {
            typed.add(GatewayBuilder.<T>castConsumer(peer));
        }
        return new FanOutConsumer<>(typed);
    }

    private static <T extends TelemetryPayload> SignalConsumer<T> guard(
            SignalConsumer<?> consumer, Provenance provenance) {
        return new GuardedConsumer<>(GatewayBuilder.<T>castConsumer(consumer), provenance);
    }

    @SuppressWarnings("unchecked")
    private static <T extends TelemetryPayload> SignalDescriptor<T> castSignal(SignalDescriptor<?> signal) {
        return (SignalDescriptor<T>) signal;
    }

    @SuppressWarnings("unchecked")
    private static <T extends TelemetryPayload> SignalConsumer<T> castConsumer(SignalConsumer<?> consumer) {
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
        var signals = new java.util.LinkedHashSet<SignalDescriptor<?>>();
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

    private java.util.Optional<ComponentId> cycleComponent(CycleException cycle) {
        return componentId(cycle.to()).or(() -> componentId(cycle.from()));
    }

    private java.util.Optional<PipelineId> firstPipeline() {
        return config.pipelines().keySet().stream().findFirst();
    }

    private static java.util.Optional<ComponentId> componentId(Node node) {
        return switch (node) {
            case ReceiverNode r -> java.util.Optional.of(r.id());
            case ProcessorNode p -> java.util.Optional.of(p.id());
            case ExporterNode e -> java.util.Optional.of(e.id());
            case ConnectorNode c -> java.util.Optional.of(c.id());
            case FanOutNode ignored -> java.util.Optional.empty();
        };
    }
}
