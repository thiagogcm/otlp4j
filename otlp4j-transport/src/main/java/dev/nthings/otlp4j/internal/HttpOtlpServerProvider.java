package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.spi.OtlpServerProvider;
import dev.nthings.otlp4j.spi.Protocol;
import dev.nthings.otlp4j.spi.ServerTransportConfig;

/// [OtlpServerProvider] backed by OTLP/HTTP — the service this module provides for `OtlpHttpReceiver`
/// to discover via `ServiceLoader` (selected by [#protocol()]).
public final class HttpOtlpServerProvider implements OtlpServerProvider {

    /// Public no-arg constructor required by `ServiceLoader`.
    public HttpOtlpServerProvider() {}

    @Override
    public Protocol protocol() {
        return Protocol.HTTP_PROTOBUF;
    }

    @Override
    public OtlpServer create(ServerTransportConfig config, Dispatchers dispatchers) {
        return new HttpOtlpServer(config, dispatchers);
    }
}
