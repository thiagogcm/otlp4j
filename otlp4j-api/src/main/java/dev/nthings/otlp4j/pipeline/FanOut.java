package dev.nthings.otlp4j.pipeline;

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
/// The `of` factories collapse degenerate shapes: a single peer is returned directly (so its
/// lifecycle stays visible to the pipeline) and an empty peer set becomes a no-op sink that
/// **accepts and discards every batch**, matching [Source#discard()] semantics.
///
/// @param <T> the OTLP signal carried by this fan-out
public final class FanOut<T> implements Sink<T> {

    /// Accepts and drops everything; the empty fan-out. Stateless, so one instance serves all
    /// signal types.
    private static final Sink<Object> DISCARD = batch -> ConsumeResult.acceptedStage();

    private final List<Sink<? super T>> peers;

    private FanOut(List<? extends Sink<? super T>> peers) {
        this.peers = List.copyOf(peers);
    }

    /// Returns a sink fanning out to the given peers. A single peer is returned as-is; an empty
    /// peer set yields a no-op sink that accepts and discards every batch.
    ///
    /// @param peers the peer sinks
    /// @param <T>   the signal type
    /// @return the fan-out sink
    @SafeVarargs
    public static <T> Sink<T> of(Sink<? super T>... peers) {
        return FanOut.<T>of(List.of(peers));
    }

    /// Returns a sink fanning out to the given peers. A single peer is returned as-is; an empty
    /// peer set yields a no-op sink that accepts and discards every batch.
    ///
    /// @param peers the peer sinks
    /// @param <T>   the signal type
    /// @return the fan-out sink
    public static <T> Sink<T> of(List<? extends Sink<? super T>> peers) {
        Objects.requireNonNull(peers, "peers");
        return switch (peers.size()) {
            case 0 -> narrow(DISCARD);
            // The bare peer, not a wrapping lambda: a lambda would hide the peer's lifecycle
            // from the pipeline's auto-collection.
            case 1 -> narrow(peers.getFirst());
            default -> new FanOut<T>(peers);
        };
    }

    /// A `Sink<? super T>` is safe to use as a `Sink<T>`: sinks only consume `T`.
    @SuppressWarnings("unchecked")
    private static <T> Sink<T> narrow(Sink<? super T> sink) {
        return (Sink<T>) sink;
    }

    /// The peers this fan-out delivers each batch to.
    ///
    /// @return the list of peers
    public List<Sink<? super T>> peers() {
        return peers;
    }

    @Override
    public CompletionStage<ConsumeResult> consume(T batch) {
        @SuppressWarnings("unchecked")
        CompletableFuture<ConsumeResult>[] futures = new CompletableFuture[peers.size()];
        for (var i = 0; i < peers.size(); i++) {
            final var idx = i;
            CompletionStage<ConsumeResult> peerStage;
            try {
                peerStage = peers.get(i).consume(batch);
            } catch (Throwable t) {
                peerStage = CompletableFuture.completedFuture(ConsumeResult.permanent(
                        "peer[" + idx + "] threw: " + describe(t), t));
            }
            futures[i] = peerStage
                    .exceptionally(t -> ConsumeResult.permanent(
                            "peer[" + idx + "] failed: " + describe(t), t))
                    .toCompletableFuture();
        }
        return CompletableFuture.allOf(futures).thenApply(v -> {
            var results = new ArrayList<ConsumeResult>(futures.length);
            for (var f : futures) {
                results.add(f.join());
            }
            return ConsumeResult.fanOutMerge(results);
        });
    }

    /// Describes a throwable, unwrapping [CompletionException] for visibility of the underlying cause.
    private static String describe(Throwable t) {
        var cause = t instanceof CompletionException ? t.getCause() : t;
        return Objects.requireNonNullElse(cause, t).toString();
    }
}
