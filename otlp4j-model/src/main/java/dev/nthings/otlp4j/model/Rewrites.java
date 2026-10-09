package dev.nthings.otlp4j.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.jspecify.annotations.Nullable;

/// The identity-preserving list rewrites behind every batch's `map…` and `filter…` operations. Each
/// returns the `source` list itself when no element changed, so a group or batch whose contents are
/// untouched can be returned as the same instance rather than a copy.
final class Rewrites {

    private Rewrites() {}

    /// Replaces every element with `mapper`'s result, which must be non-null.
    static <E> List<E> map(List<E> source, UnaryOperator<E> mapper) {
        return rewrite(source, element -> Objects.requireNonNull(mapper.apply(element), "mapper returned null"));
    }

    /// Keeps the elements `keep` accepts.
    static <E> List<E> filter(List<E> source, Predicate<? super E> keep) {
        return rewrite(source, element -> keep.test(element) ? element : null);
    }

    /// Rewrites every group and removes a group that the rewrite changed and left `empty`. A group the
    /// rewrite returns unchanged stays, even when it was already empty.
    static <G> List<G> prune(List<G> source, UnaryOperator<G> rewrite, Predicate<G> empty) {
        return rewrite(source, group -> {
            var rewritten = rewrite.apply(group);
            return rewritten != group && empty.test(rewritten) ? null : rewritten;
        });
    }

    /// Applies `step` to every element in order: the same instance keeps the element, another instance
    /// replaces it, and `null` removes it.
    private static <E> List<E> rewrite(List<E> source, Function<E, @Nullable E> step) {
        List<E> changed = null;
        for (var i = 0; i < source.size(); i++) {
            var element = source.get(i);
            var replacement = step.apply(element);
            if (changed == null) {
                if (replacement == element) {
                    continue;
                }
                changed = new ArrayList<>(source.size());
                changed.addAll(source.subList(0, i));
            }
            if (replacement != null) {
                changed.add(replacement);
            }
        }
        return changed == null ? source : changed;
    }
}
