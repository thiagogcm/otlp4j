package dev.nthings.otlp4j.components.routing;

import dev.nthings.otlp4j.core.consumer.ConsumeContext;
import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.RoutingMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;

/// One conjunction of resource-attribute, client-header, and optional domain conditions. A rule
/// without conditions matches every payload; that explicit rule is the building block for a catch-all
/// [MatchSpec]. Routing and header checks stay on [Payload#routingMetadata()] and never decode the
/// domain. A domain predicate is the deliberate opt-in to [Payload#data()].
public final class MatchRule {

    private static final MatchRule MATCH_ALL = new MatchRule(List.of(), List.of(), Optional.empty());

    private final List<Condition> resourceAttributes;
    private final List<Condition> headers;
    private final Optional<Predicate<Object>> domainPredicate;

    private MatchRule(
            List<Condition> resourceAttributes,
            List<Condition> headers,
            Optional<Predicate<Object>> domainPredicate) {
        this.resourceAttributes = List.copyOf(resourceAttributes);
        this.headers = List.copyOf(headers);
        this.domainPredicate = domainPredicate;
    }

    /// Returns an explicit catch-all rule.
    public static MatchRule matchAll() {
        return MATCH_ALL;
    }

    /// Starts building a conjunction.
    public static Builder builder() {
        return new Builder();
    }

    /// Returns whether every condition holds.
    public boolean matches(Payload<?> payload, ConsumeContext context) {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(context, "context");
        if (!resourceAttributes.isEmpty()) {
            RoutingMetadata metadata = payload.routingMetadata();
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
        return domainPredicate.isEmpty() || domainPredicate.get().test(payload.data());
    }

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
            return compiled == null ? pattern.equals(candidate) : compiled.matcher(candidate).matches();
        }
    }

    /// A builder for one all-of rule.
    public static final class Builder {
        private final List<Condition> resourceAttributes = new ArrayList<>();
        private final List<Condition> headers = new ArrayList<>();
        private Optional<Predicate<Object>> domainPredicate = Optional.empty();

        private Builder() {}

        /// Requires an exact resource attribute value.
        public Builder resourceAttribute(String key, String value) {
            resourceAttributes.add(Condition.exact(key, value));
            return this;
        }

        /// Requires a full-match resource attribute regular expression.
        public Builder resourceAttributeRegex(String key, String pattern) {
            resourceAttributes.add(Condition.regex(key, pattern));
            return this;
        }

        /// Requires at least one exact client-header value.
        public Builder header(String key, String value) {
            headers.add(Condition.exact(key, value));
            return this;
        }

        /// Requires at least one client-header value matching a full regular expression.
        public Builder headerRegex(String key, String pattern) {
            headers.add(Condition.regex(key, pattern));
            return this;
        }

        /// Adds the deliberate domain-materialising escape hatch.
        public Builder domainPredicate(Predicate<Object> predicate) {
            if (predicate == null) {
                throw new IllegalArgumentException("domain predicate must be non-null");
            }
            domainPredicate = Optional.of(predicate);
            return this;
        }

        /// Builds an immutable rule.
        public MatchRule build() {
            return new MatchRule(resourceAttributes, headers, domainPredicate);
        }
    }
}
