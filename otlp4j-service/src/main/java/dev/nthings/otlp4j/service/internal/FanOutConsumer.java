package dev.nthings.otlp4j.service.internal;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.consumer.ConsumerCapabilities;
import dev.nthings.otlp4j.core.consumer.SignalConsumer;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// The runtime realization of a [dev.nthings.otlp4j.service.internal.graph.FanOutNode]: a consumer
/// that delivers one payload to every downstream peer and merges their results with
/// [DeliveryResult#fanOutMerge], preserving the merged provenance and downstream marker. Each peer is
/// a guarded consumer, so a peer failure is already a delivery result rather than an escaped throw by
/// the time it reaches the merge.
///
/// A peer that declares it mutates the payload is, in the Collector fan-out rule, handed a defensive
/// copy so it cannot corrupt a sibling's view, through the payload's `copyForMutation` seam. In
/// Phase 2 both payload backings are immutable, so `copyForMutation` returns the same share-safe
/// instance and the copy is byte-for-byte identical to sharing; the deep copy arrives when a mutable
/// payload mode ships, leaving this call site unchanged. A non-mutating peer, which every exporter
/// is, always shares.
///
/// @param <T> the payload type
public final class FanOutConsumer<T extends dev.nthings.otlp4j.core.signal.TelemetryPayload>
        implements SignalConsumer<T> {

    private final List<SignalConsumer<T>> peers;

    private final boolean anyPeerMutates;

    /// Builds a fan-out over `peers`, which must be non-empty.
    ///
    /// @param peers the downstream consumers, at least one
    public FanOutConsumer(List<SignalConsumer<T>> peers) {
        if (peers == null || peers.isEmpty()) {
            throw new IllegalArgumentException("a fan-out requires at least one peer");
        }
        this.peers = List.copyOf(peers);
        var mutates = false;
        for (var peer : this.peers) {
            if (peer.capabilities().mutatesData()) {
                mutates = true;
            }
        }
        this.anyPeerMutates = mutates;
    }

    @Override
    public ConsumerCapabilities capabilities() {
        // The fan-out itself never mutates the shared payload; a mutating peer is isolated by the
        // copyForMutation seam in consume, so upstream sees a non-mutating consumer here.
        return ConsumerCapabilities.nonMutating();
    }

    @Override
    public CompletionStage<DeliveryResult> consume(ConsumeContext context, T data) {
        var stages = new ArrayList<CompletionStage<DeliveryResult>>(peers.size());
        for (var peer : peers) {
            // Copy before handing the payload to a peer that declares it mutates, so it cannot corrupt
            // a sibling's view. In Phase 2 both payload backings are immutable, so copyForMutation
            // shares the same instance and this is identical to sharing; the cast is safe because
            // copyForMutation returns a payload of the same runtime type this fan-out carries.
            @SuppressWarnings("unchecked")
            T forPeer = peer.capabilities().mutatesData() ? (T) data.copyForMutation() : data;
            stages.add(peer.consume(context, forPeer));
        }
        var futures = new CompletableFuture<?>[stages.size()];
        for (var i = 0; i < stages.size(); i++) {
            futures[i] = stages.get(i).toCompletableFuture();
        }
        return CompletableFuture.allOf(futures).thenApply(ignored -> {
            var results = new ArrayList<DeliveryResult>(stages.size());
            for (var stage : stages) {
                results.add(stage.toCompletableFuture().join());
            }
            return DeliveryResult.fanOutMerge(results);
        });
    }

    /// Whether any peer declared that it mutates the payload, exposed for diagnostics and tests.
    ///
    /// @return true if at least one peer mutates
    public boolean anyPeerMutates() {
        return anyPeerMutates;
    }
}
