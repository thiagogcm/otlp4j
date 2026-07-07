package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.ComponentConfig;
import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;
import dev.nthings.otlp4j.core.component.Connector;
import dev.nthings.otlp4j.core.component.ConnectorFactory;
import dev.nthings.otlp4j.core.component.CreateContext;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.ExporterFactory;
import dev.nthings.otlp4j.core.component.Extension;
import dev.nthings.otlp4j.core.component.ExtensionFactory;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.NetworkBound;
import dev.nthings.otlp4j.core.component.Processor;
import dev.nthings.otlp4j.core.component.ProcessorFactory;
import dev.nthings.otlp4j.core.component.Receiver;
import dev.nthings.otlp4j.core.component.ReceiverFactory;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.component.StabilityLevel;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.consumer.SignalConsumers;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.SignalPair;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.service.config.SignalSupport;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

/// Shared fake components and factories for the graph, ordering, and diagnostic tests. They record
/// their instantiation, start, and shutdown into a shared log so a test can assert the graph-derived
/// order, and they can be told to fail on start to exercise rollback. Restricting a factory's
/// supported signals exercises the signal-support diagnostics.
final class Fakes {

    private Fakes() {}

    /// A shared, thread-safe record of lifecycle events across a set of fakes.
    static final class Log {
        private final List<String> events = new CopyOnWriteArrayList<>();

        void record(String event) {
            events.add(event);
        }

        List<String> events() {
            return List.copyOf(events);
        }
    }

    /// The one config shared by every fake, carrying the lifecycle-behaviour switches a test toggles.
    ///
    /// @param failStart    whether start fails
    /// @param failShutdown whether shutdown fails
    /// @param failFlush    whether force-flush fails
    /// @param hangStart    whether start returns a stage that never completes (to force a timeout)
    record Config(boolean failStart, boolean failShutdown, boolean failFlush, boolean hangStart)
            implements ComponentConfig {
        static Config ok() {
            return new Config(false, false, false, false);
        }

        static Config failingStart() {
            return new Config(true, false, false, false);
        }

        static Config failingShutdown() {
            return new Config(false, true, false, false);
        }

        static Config failingFlush() {
            return new Config(false, false, true, false);
        }

        static Config hangingStart() {
            return new Config(false, false, false, true);
        }
    }

    private static CompletionStage<Void> startStage(Log log, ComponentId id, Config config) {
        log.record("start:" + id);
        if (config.hangStart()) {
            return new CompletableFuture<>();
        }
        if (config.failStart()) {
            return CompletableFuture.failedFuture(new IllegalStateException("start of " + id + " failed on purpose"));
        }
        return CompletableFuture.completedFuture(null);
    }

    private static CompletionStage<Void> shutdownStage(Log log, ComponentId id, Config config) {
        log.record("shutdown:" + id);
        if (config.failShutdown()) {
            return CompletableFuture.failedFuture(new IllegalStateException("shutdown of " + id + " failed on purpose"));
        }
        return CompletableFuture.completedFuture(null);
    }

    private static CompletionStage<Void> flushStage(Log log, ComponentId id, Config config) {
        log.record("flush:" + id);
        if (config.failFlush()) {
            return CompletableFuture.failedFuture(new IllegalStateException("flush of " + id + " failed on purpose"));
        }
        return CompletableFuture.completedFuture(null);
    }

    // ---- receiver --------------------------------------------------------------------------------

    static final class ReceiverImpl implements Receiver, NetworkBound {
        private final ComponentId id;
        private final Log log;
        private final Config config;
        private final SignalConsumers consumers;

        ReceiverImpl(ComponentId id, Log log, Config config, SignalConsumers consumers) {
            this.id = id;
            this.log = log;
            this.config = config;
            this.consumers = consumers;
        }

        SignalConsumers consumers() {
            return consumers;
        }

        @Override
        public CompletionStage<Void> start(ComponentContext context) {
            return startStage(log, id, config);
        }

        @Override
        public CompletionStage<Void> forceFlush(FlushContext context) {
            return flushStage(log, id, config);
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext context) {
            return shutdownStage(log, id, config);
        }

        @Override
        public int port() {
            return 45678;
        }
    }

    static final class ReceiverFactoryImpl implements ReceiverFactory<Config>, SignalSupport {
        private final String type;
        private final Log log;
        private final Set<SignalDescriptor<?>> supported;

        ReceiverFactoryImpl(String type, Log log, Set<SignalDescriptor<?>> supported) {
            this.type = type;
            this.log = log;
            this.supported = supported;
        }

        @Override
        public ComponentKind kind() {
            return ComponentKind.RECEIVER;
        }

        @Override
        public StabilityLevel stability() {
            return StabilityLevel.BETA;
        }

        @Override
        public Config defaultConfig() {
            return Config.ok();
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public boolean supportsSignal(SignalDescriptor<?> signal) {
            return supported == null || supported.contains(signal);
        }

        @Override
        public Receiver create(CreateContext ctx, Config config, SignalConsumers consumers) {
            log.record("create:" + ctx.id());
            return new ReceiverImpl(ctx.id(), log, config, consumers);
        }
    }

    // ---- processor -------------------------------------------------------------------------------

    static final class ProcessorImpl<T extends TelemetryPayload> implements Processor<T> {
        private final ComponentId id;
        private final Log log;
        private final Config config;
        private final SignalConsumer<T> next;
        private final boolean mutates;

        ProcessorImpl(ComponentId id, Log log, Config config, SignalConsumer<T> next, boolean mutates) {
            this.id = id;
            this.log = log;
            this.config = config;
            this.next = next;
            this.mutates = mutates;
        }

        @Override
        public ConsumerCapabilities capabilities() {
            return mutates ? ConsumerCapabilities.mutating() : ConsumerCapabilities.nonMutating();
        }

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext context, T data) {
            log.record("consume:" + id);
            return next.consume(context, data);
        }

        @Override
        public CompletionStage<Void> start(ComponentContext context) {
            return startStage(log, id, config);
        }

        @Override
        public CompletionStage<Void> forceFlush(FlushContext context) {
            return flushStage(log, id, config);
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext context) {
            return shutdownStage(log, id, config);
        }
    }

    static final class ProcessorFactoryImpl implements ProcessorFactory<Config>, SignalSupport {
        private final String type;
        private final Log log;
        private final Set<SignalDescriptor<?>> supported;
        private final boolean mutates;

        ProcessorFactoryImpl(String type, Log log, Set<SignalDescriptor<?>> supported) {
            this(type, log, supported, false);
        }

        ProcessorFactoryImpl(String type, Log log, Set<SignalDescriptor<?>> supported, boolean mutates) {
            this.type = type;
            this.log = log;
            this.supported = supported;
            this.mutates = mutates;
        }

        @Override
        public ComponentKind kind() {
            return ComponentKind.PROCESSOR;
        }

        @Override
        public StabilityLevel stability() {
            return StabilityLevel.BETA;
        }

        @Override
        public Config defaultConfig() {
            return Config.ok();
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public boolean supportsSignal(SignalDescriptor<?> signal) {
            return supported == null || supported.contains(signal);
        }

        @Override
        public <T extends TelemetryPayload> Processor<T> create(
                CreateContext ctx, Config config, SignalDescriptor<T> signal, SignalConsumer<T> next) {
            log.record("create:" + ctx.id());
            return new ProcessorImpl<>(ctx.id(), log, config, next, mutates);
        }
    }

    // ---- exporter --------------------------------------------------------------------------------

    static final class ExporterImpl<T extends TelemetryPayload> implements Exporter<T> {
        private final ComponentId id;
        private final Log log;
        private final Config config;
        private final SignalDescriptor<T> signal;
        private final DeliveryResult outcome;

        ExporterImpl(ComponentId id, Log log, Config config, SignalDescriptor<T> signal, DeliveryResult outcome) {
            this.id = id;
            this.log = log;
            this.config = config;
            this.signal = signal;
            this.outcome = outcome;
        }

        @Override
        public ConsumerCapabilities capabilities() {
            return ConsumerCapabilities.nonMutating();
        }

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext context, T data) {
            log.record("consume:" + id);
            return CompletableFuture.completedFuture(
                    outcome != null ? outcome : DeliveryResult.accepted(provenance()));
        }

        private Provenance provenance() {
            return new Provenance(id, ComponentKind.EXPORTER, signal);
        }

        @Override
        public CompletionStage<Void> start(ComponentContext context) {
            return startStage(log, id, config);
        }

        @Override
        public CompletionStage<Void> forceFlush(FlushContext context) {
            return flushStage(log, id, config);
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext context) {
            return shutdownStage(log, id, config);
        }
    }

    static final class ExporterFactoryImpl implements ExporterFactory<Config>, SignalSupport {
        private final String type;
        private final Log log;
        private final Set<SignalDescriptor<?>> supported;
        private final DeliveryResult outcome;

        ExporterFactoryImpl(String type, Log log, Set<SignalDescriptor<?>> supported, DeliveryResult outcome) {
            this.type = type;
            this.log = log;
            this.supported = supported;
            this.outcome = outcome;
        }

        @Override
        public ComponentKind kind() {
            return ComponentKind.EXPORTER;
        }

        @Override
        public StabilityLevel stability() {
            return StabilityLevel.BETA;
        }

        @Override
        public Config defaultConfig() {
            return Config.ok();
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public boolean supportsSignal(SignalDescriptor<?> signal) {
            return supported == null || supported.contains(signal);
        }

        @Override
        public <T extends TelemetryPayload> Exporter<T> create(
                CreateContext ctx, Config config, SignalDescriptor<T> signal) {
            log.record("create:" + ctx.id());
            return new ExporterImpl<>(ctx.id(), log, config, signal, outcome);
        }
    }

    // ---- connector -------------------------------------------------------------------------------

    static final class ConnectorImpl<I extends TelemetryPayload> implements Connector, SignalConsumer<I> {
        private final ComponentId id;
        private final Log log;
        private final Config config;
        private final SignalPair<?, ?> pair;

        ConnectorImpl(ComponentId id, Log log, Config config, SignalPair<?, ?> pair) {
            this.id = id;
            this.log = log;
            this.config = config;
            this.pair = pair;
        }

        @Override
        public ConsumerCapabilities capabilities() {
            return ConsumerCapabilities.nonMutating();
        }

        @Override
        public CompletionStage<DeliveryResult> consume(ConsumeContext context, I data) {
            log.record("consume:" + id);
            return CompletableFuture.completedFuture(
                    DeliveryResult.accepted(new Provenance(id, ComponentKind.CONNECTOR, pair.from())));
        }

        @Override
        public CompletionStage<Void> start(ComponentContext context) {
            return startStage(log, id, config);
        }

        @Override
        public CompletionStage<Void> forceFlush(FlushContext context) {
            return flushStage(log, id, config);
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext context) {
            return shutdownStage(log, id, config);
        }
    }

    static final class ConnectorFactoryImpl implements ConnectorFactory<Config> {
        private final String type;
        private final Log log;
        private final Set<SignalPair<?, ?>> pairs;

        ConnectorFactoryImpl(String type, Log log, Set<SignalPair<?, ?>> pairs) {
            this.type = type;
            this.log = log;
            this.pairs = pairs;
        }

        @Override
        public ComponentKind kind() {
            return ComponentKind.CONNECTOR;
        }

        @Override
        public StabilityLevel stability() {
            return StabilityLevel.BETA;
        }

        @Override
        public Config defaultConfig() {
            return Config.ok();
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public Set<SignalPair<?, ?>> supportedPairs() {
            return pairs;
        }

        @Override
        public Connector create(CreateContext ctx, Config config, SignalPair<?, ?> pair, SignalConsumer<?> next) {
            log.record("create:" + ctx.id());
            return new ConnectorImpl<>(ctx.id(), log, config, pair);
        }
    }

    // ---- extension -------------------------------------------------------------------------------

    static final class ExtensionImpl implements Extension {
        private final ComponentId id;
        private final Log log;
        private final Config config;

        ExtensionImpl(ComponentId id, Log log, Config config) {
            this.id = id;
            this.log = log;
            this.config = config;
        }

        @Override
        public CompletionStage<Void> start(ComponentContext context) {
            return startStage(log, id, config);
        }

        @Override
        public CompletionStage<Void> forceFlush(FlushContext context) {
            return flushStage(log, id, config);
        }

        @Override
        public CompletionStage<Void> shutdown(ShutdownContext context) {
            return shutdownStage(log, id, config);
        }
    }

    static final class ExtensionFactoryImpl implements ExtensionFactory<Config> {
        private final String type;
        private final Log log;

        ExtensionFactoryImpl(String type, Log log) {
            this.type = type;
            this.log = log;
        }

        @Override
        public ComponentKind kind() {
            return ComponentKind.EXTENSION;
        }

        @Override
        public StabilityLevel stability() {
            return StabilityLevel.BETA;
        }

        @Override
        public Config defaultConfig() {
            return Config.ok();
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public Extension create(CreateContext ctx, Config config) {
            log.record("create:" + ctx.id());
            return new ExtensionImpl(ctx.id(), log, config);
        }
    }

    static Set<SignalPair<?, ?>> pairs(SignalPair<?, ?>... p) {
        return Set.of(p);
    }

    static Set<SignalDescriptor<?>> signals(SignalDescriptor<?>... s) {
        return Collections.unmodifiableSet(new java.util.LinkedHashSet<>(List.of(s)));
    }
}
