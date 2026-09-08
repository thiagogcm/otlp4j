package dev.nthings.otlp4j.service.internal;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.signal.Payload;
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
/// Peers share the immutable payload. Transformations replace it in their own branch, so they
/// cannot change the data observed by a sibling.
///
/// @param <T> the signal's domain data type
public final class FanOutConsumer<T>
        implements SignalConsumer<T> {

    private final List<SignalConsumer<T>> peers;

    /// Builds a fan-out over `peers`, which must be non-empty.
    ///
    /// @param peers the downstream consumers, at least one
    public FanOutConsumer(List<SignalConsumer<T>> peers) {
        if (peers == null || peers.isEmpty()) {
            throw new IllegalArgumentException("a fan-out requires at least one peer");
        }
        this.peers = List.copyOf(peers);
    }

    @Override
    public CompletionStage<DeliveryResult> consume(ConsumeContext context, Payload<T> data) {
        var stages = new ArrayList<CompletionStage<DeliveryResult>>(peers.size());
        for (var peer : peers) {
            stages.add(peer.consume(context, data));
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
}
