package dev.nthings.otlp4j.transport.http;

import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.receiver.Receiver;
import dev.nthings.otlp4j.spi.Dispatchers;
import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.transport.common.AbstractReceiverBuilder;
import dev.nthings.otlp4j.transport.http.internal.HttpOtlpServer;

/// Builds [Receiver]s that accept OTLP/HTTP requests and dispatch to per-signal sinks.
///
/// Call [Receiver#start()] on the built receiver to bind the transport. The default bind is
/// `localhost:4318` with plaintext.
public final class OtlpHttpReceiver {

    /// The conventional OTLP/HTTP port (4318); gRPC uses 4317.
    static final int DEFAULT_HTTP_PORT = 4318;

    private OtlpHttpReceiver() {}

    public static Builder builder() {
        return new Builder();
    }

    /// Builder for an OTLP/HTTP [Receiver]. Defaults bind `localhost:4318` with plaintext
    /// transport; the shared knobs live on [AbstractReceiverBuilder].
    public static final class Builder extends AbstractReceiverBuilder<Builder> {

        private Builder() {
            super(ServerConfig.builder().setPort(DEFAULT_HTTP_PORT), "OTLP/HTTP");
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected OtlpServer newServer(ServerConfig config, Dispatchers dispatchers) {
            return new HttpOtlpServer(config, dispatchers);
        }
    }
}
