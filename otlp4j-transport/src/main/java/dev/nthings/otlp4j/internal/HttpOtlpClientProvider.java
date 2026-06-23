package dev.nthings.otlp4j.internal;

import dev.nthings.otlp4j.spi.ClientTransportConfig;
import dev.nthings.otlp4j.spi.OtlpClient;
import dev.nthings.otlp4j.spi.OtlpClientProvider;
import dev.nthings.otlp4j.spi.Protocol;

/// [OtlpClientProvider] backed by OTLP/HTTP — the service this module provides for `OtlpHttpExporter`
/// to discover via `ServiceLoader` (selected by [#protocol()]).
public final class HttpOtlpClientProvider implements OtlpClientProvider {

    /// Public no-arg constructor required by `ServiceLoader`.
    public HttpOtlpClientProvider() {}

    @Override
    public Protocol protocol() {
        return Protocol.HTTP_PROTOBUF;
    }

    @Override
    public OtlpClient create(ClientTransportConfig config) {
        return new HttpOtlpClient(config);
    }
}
