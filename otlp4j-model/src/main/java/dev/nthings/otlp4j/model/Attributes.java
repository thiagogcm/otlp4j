package dev.nthings.otlp4j.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/// An immutable, insertion-ordered collection of OpenTelemetry attributes (string keys to
/// [AttributeValue]s), decoupled from the wire `repeated KeyValue` representation.
public final class Attributes {

    private static final Attributes EMPTY = new Attributes(Collections.emptyMap());

    private final Map<String, AttributeValue> values;

    private Attributes(Map<String, AttributeValue> values) {
        this.values = values;
    }

    public static Attributes empty() {
        return EMPTY;
    }

    /// Wraps a copy of the given map, preserving its iteration order.
    public static Attributes of(Map<String, AttributeValue> values) {
        if (values.isEmpty()) {
            return EMPTY;
        }
        return new Attributes(Collections.unmodifiableMap(new LinkedHashMap<>(values)));
    }

    public static Builder builder() {
        return new Builder();
    }

    public Optional<AttributeValue> get(String key) {
        return Optional.ofNullable(values.get(key));
    }

    /// Returns the string value of `key`, or `null` if absent or not a string.
    public String getString(String key) {
        return values.get(key) instanceof AttributeValue.StringValue s ? s.value() : null;
    }

    public boolean contains(String key) {
        return values.containsKey(key);
    }

    public Set<String> keys() {
        return values.keySet();
    }

    /// The attributes as an unmodifiable, insertion-ordered map.
    public Map<String, AttributeValue> asMap() {
        return values;
    }

    public int size() {
        return values.size();
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Attributes other && values.equals(other.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public String toString() {
        return "Attributes" + values;
    }

    public static final class Builder {
        private final Map<String, AttributeValue> values = new LinkedHashMap<>();

        private Builder() {}

        public Builder put(String key, AttributeValue value) {
            values.put(key, value);
            return this;
        }

        public Builder put(String key, String value) {
            return put(key, AttributeValue.of(value));
        }

        public Builder put(String key, long value) {
            return put(key, AttributeValue.of(value));
        }

        public Builder put(String key, double value) {
            return put(key, AttributeValue.of(value));
        }

        public Builder put(String key, boolean value) {
            return put(key, AttributeValue.of(value));
        }

        public Attributes build() {
            return Attributes.of(values);
        }
    }
}
