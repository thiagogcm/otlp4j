package dev.nthings.otlp4j.model;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// A typed OpenTelemetry attribute value, decoupled from the wire (`AnyValue`) representation.
///
/// An attribute value is one of: a string, boolean, signed 64-bit integer, double, byte string,
/// a heterogeneous array, a nested key/value map, or [Empty] (no value set).
public sealed interface AttributeValue {

    record StringValue(String value) implements AttributeValue {}

    record BoolValue(boolean value) implements AttributeValue {}

    record LongValue(long value) implements AttributeValue {}

    record DoubleValue(double value) implements AttributeValue {}

    /// A byte string. The wrapped array is defensively copied on construction and access.
    record BytesValue(byte[] value) implements AttributeValue {
        public BytesValue {
            value = value.clone();
        }

        @Override
        public byte[] value() {
            return value.clone();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof BytesValue other && Arrays.equals(value, other.value);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }

        @Override
        public String toString() {
            return "BytesValue[" + value.length + " bytes]";
        }
    }

    /// A heterogeneous array, defensively copied on construction.
    record ArrayValue(List<AttributeValue> values) implements AttributeValue {
        public ArrayValue {
            values = List.copyOf(values);
        }
    }

    /// A nested key/value map, defensively copied on construction (iteration order preserved).
    record KeyValueListValue(Map<String, AttributeValue> values) implements AttributeValue {
        public KeyValueListValue {
            values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
        }
    }

    /// The "empty" value: an attribute value with none of its variants set.
    record Empty() implements AttributeValue {}

    Empty EMPTY = new Empty();

    static AttributeValue of(String value) {
        return new StringValue(value);
    }

    static AttributeValue of(boolean value) {
        return new BoolValue(value);
    }

    static AttributeValue of(long value) {
        return new LongValue(value);
    }

    static AttributeValue of(double value) {
        return new DoubleValue(value);
    }

    static AttributeValue of(byte[] value) {
        return new BytesValue(value);
    }

    static AttributeValue of(List<AttributeValue> values) {
        return new ArrayValue(values);
    }

    /// Wraps a copy of the map, preserving iteration order.
    static AttributeValue of(Map<String, AttributeValue> values) {
        return new KeyValueListValue(values);
    }

    static AttributeValue empty() {
        return EMPTY;
    }
}
