package dev.nthings.otlp4j.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/// A [Consumer] that delivers the same batch to several peers concurrently.
///
/// All peers run in parallel; their results are aggregated with [ConsumeResult#fanOutMerge]
/// (worst-case rejection count, not sum). A peer that throws is captured as a per-peer
/// [ConsumeResult.Rejected] and never blocks the other peers.
///
/// The same `batch` reference is shared with every peer; the OTLP model is immutable, so this
/// is safe even when a peer declares [Capabilities#MUTATES_DATA]. The capability flag is still
/// propagated upward should the model ever gain mutable fields.
///
/// @param <T> the OTLP signal carried by this fan-out
public final class FanOut<T> implements Consumer<T> {

    private final List<Consumer<T>> peers;
    private final Capabilities capabilities;

    private FanOut(List<Consumer<T>> peers) {
        this.peers = List.copyOf(peers);
        this.capabilities =
                this.peers.stream().anyMatch(p -> p.capabilities() == Capabilities.MUTATES_DATA)
                        ? Capabilities.MUTATES_DATA
                        : Capabilities.IMMUTABLE;
    }

    /// Returns a [FanOut] over the given peers. Peers run concurrently.
    @SafeVarargs
    public static <T> FanOut<T> of(Consumer<T>... peers) {
        return new FanOut<>(List.of(peers));
    }

    /// Returns a [FanOut] over the given peers. Peers run concurrently.
    public static <T> FanOut<T> of(List<? extends Consumer<T>> peers) {
        Objects.requireNonNull(peers, "peers");
        return new FanOut<>(new ArrayList<>(peers));
    }

    /// The peers this fan-out delivers each batch to.
    public List<Consumer<T>> peers() {
        return peers;
    }

    @Override
    public CompletionStage<ConsumeResult<T>> consume(T batch) {
        if (peers.isEmpty()) {
            return ConsumeResult.acceptedStage();
        }
        @SuppressWarnings("unchecked")
        CompletableFuture<ConsumeResult<T>>[] futures = new CompletableFuture[peers.size()];
        for (int i = 0; i < peers.size(); i++) {
            final int idx = i;
            CompletionStage<ConsumeResult<T>> peerStage;
            try {
                peerStage = peers.get(i).consume(batch);
            } catch (RuntimeException e) {
                peerStage = CompletableFuture.completedFuture(ConsumeResult.rejected(
                        "peer[" + idx + "] threw: " + e.getMessage(), e));
            }
            futures[i] = peerStage
                    .exceptionally(t -> ConsumeResult.<T>rejected(
                            "peer[" + idx + "] failed: " + t.getMessage(), t))
                    .toCompletableFuture();
        }
        return CompletableFuture.allOf(futures).thenApply(v -> {
            var results = new ArrayList<ConsumeResult<T>>(futures.length);
            for (var f : futures) {
                results.add(f.join());
            }
            return ConsumeResult.fanOutMerge(results);
        });
    }

    @Override
    public Capabilities capabilities() {
        return capabilities;
    }
}
