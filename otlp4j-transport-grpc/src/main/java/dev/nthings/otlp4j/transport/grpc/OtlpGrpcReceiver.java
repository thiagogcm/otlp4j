package dev.nthings.otlp4j.transport.grpc;

import dev.nthings.otlp4j.config.ServerConfig;
import dev.nthings.otlp4j.receiver.Receiver;
import dev.nthings.otlp4j.spi.Dispatchers;
import dev.nthings.otlp4j.spi.OtlpServer;
import dev.nthings.otlp4j.transport.common.AbstractReceiverBuilder;
import dev.nthings.otlp4j.transport.grpc.internal.GrpcOtlpServer;

/// Builds [Receiver]s that accept OTLP/gRPC requests and dispatch to per-signal sinks.
///
/// Call [Receiver#start()] on the built receiver to bind the transport. The default bind is
/// `localhost:4317`.
public final class OtlpGrpcReceiver {

    private OtlpGrpcReceiver() {}

    public static Builder builder() {
        return new Builder();
    }

    /// Builder for an OTLP/gRPC [Receiver]. Defaults bind `localhost:4317` with plaintext
    /// transport; the shared knobs live on [AbstractReceiverBuilder].
    public static final class Builder extends AbstractReceiverBuilder<Builder> {

        private Builder() {
            super(ServerConfig.builder(), "OTLP/gRPC");
        }

        /// Caps in-flight calls per connection; `0` (default) is unlimited.
        ///
        /// @param max the per-connection cap
        /// @return this builder
        public Builder setMaxConcurrentCallsPerConnection(int max) {
            config().setMaxConcurrentCallsPerConnection(max);
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected OtlpServer newServer(ServerConfig config, Dispatchers dispatchers) {
            return new GrpcOtlpServer(config, dispatchers);
        }
    }
}
