package dev.nthings.otlp4j.spi;

import java.util.Objects;

/// Configuration for an OTLP receiver transport.
public record ServerTransportConfig(String bindHost, int port, Tls tls) {

    public ServerTransportConfig {
        Objects.requireNonNull(bindHost, "bindHost");
        Objects.requireNonNull(tls, "tls");
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Returns a builder pre-populated with this config's fields.
    public Builder toBuilder() {
        return new Builder().bindHost(bindHost).port(port).tls(tls);
    }

    public static final class Builder {

        private String bindHost = "0.0.0.0";
        private int port = 4317;
        private Tls tls = Tls.disabled();

        private Builder() {}

        public Builder bindHost(String host) {
            this.bindHost = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder tls(Tls tls) {
            this.tls = tls;
            return this;
        }

        public ServerTransportConfig build() {
            return new ServerTransportConfig(bindHost, port, tls);
        }
    }
}
