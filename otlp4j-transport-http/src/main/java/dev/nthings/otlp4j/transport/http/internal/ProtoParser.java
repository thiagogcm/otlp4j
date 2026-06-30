package dev.nthings.otlp4j.transport.http.internal;

import com.google.protobuf.InvalidProtocolBufferException;

/// Parses a protobuf message from its binary encoding - a method reference to `parseFrom(byte[])`.
/// Shared by the HTTP client (response bodies) and handlers (request bodies).
@FunctionalInterface
interface ProtoParser<T> {

    /// Parses `bytes`, throwing [InvalidProtocolBufferException] on invalid encoding.
    T parse(byte[] bytes) throws InvalidProtocolBufferException;
}
