package dev.nthings.otlp4j.pipeline;

import dev.nthings.otlp4j.core.Sink;
import dev.nthings.otlp4j.model.ConsumeResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/// A [Sink] that delivers the same batch to several peers concurrently.
///
/// All peers run in parallel; their results are aggregated with [ConsumeResult#fanOutMerge]
/// (worst-case rejection count, not sum). A peer that throws is captured as a per-peer
/// [ConsumeResult.Rejected] and never blocks the other peers.
///
/// The same `batch` reference is shared with every peer; the OTLP model is immutable, so
/// concurrent delivery is safe.
///
/// @param <T> the OTLP signal carried by this fan-out
public final class FanOut<T> implements Sink<T> {

    private final List<Sink<? super T>> peers;

    private FanOut(List<? extends Sink<? super T>> peers) {
        if (peers.isEmpty()) {
            throw new IllegalArgumentException("FanOut requires at least one peer");
        }
        this.peers = List.copyOf(peers);
    }

    /// Returns a [FanOut] over the given peers. Peers run concurrently; at least one is required.
    @SafeVarargs
    public static <T> FanOut<T> of(Sink<? super T>... peers) {
        return new FanOut<T>(List.of(peers));
    }

    /// Returns a [FanOut] over the given peers. Peers run concurrently; at least one is required.
    public static <T> FanOut<T> of(List<? extends Sink<? super T>> peers) {
        Objects.requireNonNull(peers, "peers");
        return new FanOut<T>(peers);
    }

    /// The peers this fan-out delivers each batch to.
    public List<Sink<? super T>> peers() {
        return peers;
    }

    @Override
    public CompletionStage<ConsumeResult<T>> consume(T batch) {
        @SuppressWarnings("unchecked")
        CompletableFuture<ConsumeResult<T>>[] futures = new CompletableFuture[peers.size()];
        for (var i = 0; i < peers.size(); i++) {
            final var idx = i;
            CompletionStage<ConsumeResult<T>> peerStage;
            try {
                peerStage = retag(peers.get(i).consume(batch));
            } catch (Throwable t) {
                peerStage = CompletableFuture.completedFuture(ConsumeResult.permanentRejected(
                        "peer[" + idx + "] threw: " + describe(t), t));
            }
            futures[i] = peerStage
                    .exceptionally(t -> ConsumeResult.<T>permanentRejected(
                            "peer[" + idx + "] failed: " + describe(t), t))
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

    /// Retags a peer's supertype-tagged result as `ConsumeResult<T>`; sound because [ConsumeResult]
    /// holds no `T`-typed data, so its type parameter is a phantom tag.
    @SuppressWarnings("unchecked")
    private static <T> CompletionStage<ConsumeResult<T>> retag(CompletionStage<? extends ConsumeResult<?>> stage) {
        return (CompletionStage<ConsumeResult<T>>) stage;
    }

    /// Describes a throwable for diagnostics, including its class and unwrapping the common
    /// CompletionException wrapper so the underlying cause is visible.
    private static String describe(Throwable t) {
        var cause = t instanceof CompletionException ? t.getCause() : t;
        return Objects.requireNonNullElse(cause, t).toString();
    }
}
