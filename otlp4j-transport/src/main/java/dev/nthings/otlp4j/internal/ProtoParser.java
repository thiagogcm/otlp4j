package dev.nthings.otlp4j.internal;

import com.google.protobuf.InvalidProtocolBufferException;

/// Parses a protobuf message from its binary encoding — a method reference to a generated
/// `parseFrom(byte[])`. Shared by the HTTP client (response bodies) and handlers (request bodies).
@FunctionalInterface
interface ProtoParser<T> {

    /// Parses `bytes`, throwing [InvalidProtocolBufferException] when they are not a valid encoding.
    T parse(byte[] bytes) throws InvalidProtocolBufferException;
}
