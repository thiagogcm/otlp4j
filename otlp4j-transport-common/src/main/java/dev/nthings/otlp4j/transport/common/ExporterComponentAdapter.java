package dev.nthings.otlp4j.transport.common;

import dev.nthings.otlp4j.core.component.ComponentContext;
import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.Exporter;
import dev.nthings.otlp4j.core.component.FlushContext;
import dev.nthings.otlp4j.core.component.SharedComponents;
import dev.nthings.otlp4j.core.component.ShutdownContext;
import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// Presents one signal facet of a shared [ClientExporter] as a core graph [Exporter].
///
/// The graph creates one exporter per pipeline-terminal signal; this adapter binds that signal and
/// forwards each payload it consumes to the exporter's matching facet, which exports through the
/// shared client and returns a [DeliveryResult] already attributed to this exporter's identity, so no
/// result bridge is needed. Because the wrapped [ClientExporter] fans all four facets over one shared
/// client channel, one channel serves every signal that terminates at one endpoint: when a single
/// exporter id terminates several signals, [#create(AbstractExporterBuilder, OtlpExporterConfig,
/// ComponentId, SignalDescriptor, ComponentContext, SharedComponents)] builds the [ClientExporter]
/// once through the build-scoped [SharedComponents] cache and hands each per-signal adapter a
/// reference-counted stake in that one shared channel, so the channel closes only when the last facet
/// shuts down.
///
/// The lifecycle maps directly: the wrapped exporter is live once built, so
/// [#start(ComponentContext)] is a no-op; [#forceFlush(FlushContext)] and [#shutdown(ShutdownContext)]
/// delegate to the exporter within their deadline.
///
/// @param <T> the payload type this exporter facet consumes
public final class ExporterComponentAdapter<T extends TelemetryPayload> implements Exporter<T> {

    private final ClientExporter exporter;

    private final SignalDescriptor<?> signal;

    private final ComponentId id;

    /// Binds one facet of `exporter` to `signal`, labelled by `id`.
    ///
    /// @param id       the exporter component identity
    /// @param signal   the signal this facet exports
    /// @param exporter the wrapped multi-signal exporter
    public ExporterComponentAdapter(ComponentId id, SignalDescriptor<?> signal, ClientExporter exporter) {
        this.exporter = exporter;
        this.signal = signal;
        this.id = id;
    }

    /// Builds an exporter adapter from a transport exporter `builder`: it applies the neutral
    /// `config`, points the exporter at the base context's metrics, attributes it to `id`, builds the
    /// [ClientExporter], and binds it to `signal`.
    ///
    /// @param builder the transport-specific exporter builder
    /// @param config  the neutral exporter config
    /// @param id      the exporter component identity
    /// @param signal  the signal this exporter facet exports
    /// @param base    the base component context (its metrics feed the export success/failure
    ///                counters)
    /// @param <T>     the payload type
    /// @return the exporter adapter
    public static <T extends TelemetryPayload> ExporterComponentAdapter<T> create(
            AbstractExporterBuilder<?> builder,
            OtlpExporterConfig config,
            ComponentId id,
            SignalDescriptor<T> signal,
            ComponentContext base) {
        builder.setConfig(config.toClientConfig());
        builder.telemetry(base.metrics());
        builder.componentId(id);
        return new ExporterComponentAdapter<>(id, signal, builder.build());
    }

    /// Builds an exporter adapter that shares one [ClientExporter] across every signal facet of `id`.
    /// The first facet of an id builds the [ClientExporter] through `shared`; later facets of the same
    /// id reuse it, so a single exporter id used by several different-signal pipelines opens exactly
    /// one wire channel. Each facet retains a reference-counted stake, so the shared channel closes
    /// only when the last facet shuts down. This is the graph-builder-provided shared-component
    /// guarantee for exporters, the mirror of a receiver's one instance across all its signals.
    ///
    /// @param builder the transport-specific exporter builder (used only when this id is built first)
    /// @param config  the neutral exporter config
    /// @param id      the exporter component identity
    /// @param signal  the signal this exporter facet exports
    /// @param base    the base component context (its metrics feed the export counters)
    /// @param shared  the build-scoped shared-component cache keyed by component id
    /// @param <T>     the payload type
    /// @return the exporter adapter, a per-signal facet over the shared client
    public static <T extends TelemetryPayload> ExporterComponentAdapter<T> create(
            AbstractExporterBuilder<?> builder,
            OtlpExporterConfig config,
            ComponentId id,
            SignalDescriptor<T> signal,
            ComponentContext base,
            SharedComponents shared) {
        ClientExporter exporter = shared.computeIfAbsent(id, () -> {
            builder.setConfig(config.toClientConfig());
            builder.telemetry(base.metrics());
            builder.componentId(id);
            return builder.build();
        });
        exporter.retain();
        return new ExporterComponentAdapter<>(id, signal, exporter);
    }

    @Override
    public ConsumerCapabilities capabilities() {
        return ConsumerCapabilities.nonMutating();
    }

    @Override
    public CompletionStage<DeliveryResult> consume(ConsumeContext context, T data) {
        return exporter.consumer(signal).consume(context, data);
    }

    @Override
    public CompletionStage<Void> start(ComponentContext context) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> forceFlush(FlushContext context) {
        return exporter.forceFlush(context.deadline());
    }

    @Override
    public CompletionStage<Void> shutdown(ShutdownContext context) {
        return exporter.shutdown(context.deadline());
    }

    @Override
    public String toString() {
        return "ExporterComponentAdapter(" + id + ", " + signal + ")";
    }
}
