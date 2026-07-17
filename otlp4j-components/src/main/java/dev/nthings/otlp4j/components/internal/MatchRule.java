package dev.nthings.otlp4j.components.internal;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.signal.PayloadView;
import dev.nthings.otlp4j.core.signal.RoutingMetadata;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;

/// The shared match engine the routing and filtering processors evaluate a payload and its consume
/// context against. One rule is a conjunction of conditions: every configured resource-attribute
/// condition, every configured client-metadata-header condition, and an optional domain predicate must
/// all hold for the rule to match. A rule with no conditions at all matches every payload, the explicit
/// building block a caller composes for a catch-all.
///
/// Evaluating the resource-attribute and header conditions reads only
/// [PayloadView#ROUTING_METADATA] and [ConsumeContext#metadata], so a rule built from those two axes
/// alone never materialises the domain batch. The domain predicate is the deliberate escape hatch for a
/// match that cannot be expressed against those two cheap axes; using it forces a
/// [PayloadView#DOMAIN_IMMUTABLE] read, and only a rule that actually carries a domain predicate pays
/// that cost. Conditions are checked resource attributes first, then headers, then the domain
/// predicate last, so a rule failing on a cheap axis never forces the expensive one.
public final class MatchRule {

    private static final MatchRule MATCH_ALL = new MatchRule(List.of(), List.of(), Optional.empty());

    private final List<Condition> resourceAttributes;
    private final List<Condition> headers;
    private final Optional<Predicate<Object>> domainPredicate;

    private MatchRule(
            List<Condition> resourceAttributes, List<Condition> headers,
            Optional<Predicate<Object>> domainPredicate) {
        this.resourceAttributes = List.copyOf(resourceAttributes);
        this.headers = List.copyOf(headers);
        this.domainPredicate = domainPredicate;
    }

    /// A rule with no conditions, which matches every payload. The explicit building block for a
    /// designated catch-all alternative in a [MatchSpec].
    ///
    /// @return the match-all rule
    public static MatchRule matchAll() {
        return MATCH_ALL;
    }

    /// A new builder for a rule combining resource-attribute, header, and domain-predicate conditions.
    ///
    /// @return the builder
    public static Builder builder() {
        return new Builder();
    }

    /// Whether this rule matches `payload` under `context`: every configured resource-attribute
    /// condition, every configured header condition, and the domain predicate, if any, must all hold. A
    /// rule with no conditions at all matches unconditionally.
    ///
    /// @param payload the payload to test
    /// @param context the consume context carrying the client metadata to test
    /// @return true when every configured condition holds
    public boolean matches(TelemetryPayload payload, ConsumeContext context) {
        if (!resourceAttributes.isEmpty()) {
            RoutingMetadata metadata = payload.view(PayloadView.ROUTING_METADATA);
            for (var condition : resourceAttributes) {
                var value = metadata.resourceAttribute(condition.key());
                if (value.isEmpty() || !condition.matches(value.get())) {
                    return false;
                }
            }
        }
        if (!headers.isEmpty()) {
            var headerValues = context.metadata().headers();
            for (var condition : headers) {
                var values = headerValues.getOrDefault(condition.key(), List.of());
                if (values.stream().noneMatch(condition::matches)) {
                    return false;
                }
            }
        }
        if (domainPredicate.isPresent()) {
            Object domain = payload.view(PayloadView.DOMAIN_IMMUTABLE);
            if (!domainPredicate.get().test(domain)) {
                return false;
            }
        }
        return true;
    }

    /// One exact-match or regular-expression-match condition on a named key, shared by the
    /// resource-attribute and header axes. The regular expression, when present, is compiled eagerly so
    /// a malformed pattern fails at rule-construction time rather than on the first match.
    private static final class Condition {
        private final String key;
        private final String pattern;
        private final @Nullable Pattern compiled;

        private Condition(String key, String pattern, boolean regex) {
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("condition key must be non-blank");
            }
            if (pattern == null) {
                throw new IllegalArgumentException("condition pattern must be non-null");
            }
            this.key = key;
            this.pattern = pattern;
            this.compiled = regex ? Pattern.compile(pattern) : null;
        }

        static Condition exact(String key, String value) {
            return new Condition(key, value, false);
        }

        static Condition regex(String key, String pattern) {
            return new Condition(key, pattern, true);
        }

        String key() {
            return key;
        }

        boolean matches(String candidate) {
            return compiled != null ? compiled.matcher(candidate).matches() : pattern.equals(candidate);
        }
    }

    /// Builds a [MatchRule] from resource-attribute, header, and domain-predicate conditions, every one
    /// of which must hold for the built rule to match.
    public static final class Builder {
        private final List<Condition> resourceAttributes = new ArrayList<>();
        private final List<Condition> headers = new ArrayList<>();
        private Optional<Predicate<Object>> domainPredicate = Optional.empty();

        private Builder() {
        }

        /// Requires the resource attribute `key` to equal `value` exactly.
        ///
        /// @param key   the resource-attribute key
        /// @param value the required exact value
        /// @return this builder
        public Builder resourceAttribute(String key, String value) {
            resourceAttributes.add(Condition.exact(key, value));
            return this;
        }

        /// Requires the resource attribute `key` to match the regular expression `pattern` in full, the
        /// same whole-string semantics as `String.matches`.
        ///
        /// @param key     the resource-attribute key
        /// @param pattern the regular expression the value must fully match
        /// @return this builder
        public Builder resourceAttributeRegex(String key, String pattern) {
            resourceAttributes.add(Condition.regex(key, pattern));
            return this;
        }

        /// Requires at least one value of the client-metadata header `key` to equal `value` exactly.
        ///
        /// @param key   the header key
        /// @param value the required exact value
        /// @return this builder
        public Builder header(String key, String value) {
            headers.add(Condition.exact(key, value));
            return this;
        }

        /// Requires at least one value of the client-metadata header `key` to match the regular
        /// expression `pattern` in full.
        ///
        /// @param key     the header key
        /// @param pattern the regular expression a value must fully match
        /// @return this builder
        public Builder headerRegex(String key, String pattern) {
            headers.add(Condition.regex(key, pattern));
            return this;
        }

        /// Sets the domain-predicate escape hatch, tested against the payload's materialised domain
        /// batch (the [PayloadView#DOMAIN_IMMUTABLE] view, narrowed by the caller to the signal's batch
        /// type). Using this forces domain materialisation whenever this rule is evaluated and the
        /// resource-attribute and header conditions, if any, already hold.
        ///
        /// @param predicate the predicate to test against the materialised domain batch
        /// @return this builder
        public Builder domainPredicate(Predicate<Object> predicate) {
            if (predicate == null) {
                throw new IllegalArgumentException("domain predicate must be non-null");
            }
            this.domainPredicate = Optional.of(predicate);
            return this;
        }

        /// Builds the rule.
        ///
        /// @return the built rule
        public MatchRule build() {
            return new MatchRule(resourceAttributes, headers, domainPredicate);
        }
    }
}
