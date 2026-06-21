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
/// The same `batch` reference is shared with every peer; the OTLP model is immutable, so
/// concurrent delivery is safe.
///
/// @param <T> the OTLP signal carried by this fan-out
public final class FanOut<T> implements Consumer<T> {

    private final List<Consumer<T>> peers;

    private FanOut(List<Consumer<T>> peers) {
        if (peers.isEmpty()) {
            throw new IllegalArgumentException("FanOut requires at least one peer");
        }
        this.peers = List.copyOf(peers);
    }

    /// Returns a [FanOut] over the given peers. Peers run concurrently; at least one is required.
    @SafeVarargs
    public static <T> FanOut<T> of(Consumer<T>... peers) {
        return new FanOut<>(List.of(peers));
    }

    /// Returns a [FanOut] over the given peers. Peers run concurrently; at least one is required.
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
}
