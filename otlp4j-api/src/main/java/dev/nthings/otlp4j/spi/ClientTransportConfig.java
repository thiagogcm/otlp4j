package dev.nthings.otlp4j.spi;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

/// Configuration for an OTLP exporter transport.
///
/// Defaults via [#builder]: `localhost:4317`, 10s deadline, TLS disabled, no headers,
/// no compression, no retries.
public record ClientTransportConfig(
        String host,
        int port,
        Duration timeout,
        Tls tls,
        Map<String, String> headers,
        Compression compression,
        RetryPolicy retry) {

    public ClientTransportConfig {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(tls, "tls");
        Objects.requireNonNull(compression, "compression");
        Objects.requireNonNull(retry, "retry");
        headers = headers == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(headers));
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be > 0");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /// Returns a builder pre-populated with this config's fields.
    public Builder toBuilder() {
        var b = new Builder();
        b.host = host;
        b.port = port;
        b.timeout = timeout;
        b.tls = tls;
        b.headers.putAll(headers);
        b.compression = compression;
        b.retry = retry;
        return b;
    }

    public static final class Builder {

        private String host = "localhost";
        private int port = 4317;
        private Duration timeout = Duration.ofSeconds(10);
        private Tls tls = Tls.disabled();
        private final Map<String, String> headers = new LinkedHashMap<>();
        private Compression compression = Compression.NONE;
        private RetryPolicy retry = RetryPolicy.none();

        private Builder() {}

        public Builder endpoint(String host, int port) {
            this.host = host;
            this.port = port;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder timeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder tls(Tls tls) {
            this.tls = tls;
            return this;
        }

        public Builder header(String key, String value) {
            this.headers.put(key, value);
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            this.headers.clear();
            this.headers.putAll(headers);
            return this;
        }

        public Builder compression(Compression compression) {
            this.compression = compression;
            return this;
        }

        public Builder retry(RetryPolicy retry) {
            this.retry = retry;
            return this;
        }

        /// Applies the standard general `OTEL_EXPORTER_OTLP_*` exporter variables onto this builder
        /// when present (endpoint URL, timeout, headers, compression, TLS cert paths). Opt-in and
        /// deterministic — the process environment is read only on this call. Call it first:
        /// explicit setters invoked afterwards override the environment. Malformed values throw
        /// [IllegalArgumentException].
        public Builder fromEnvironment() {
            return fromEnvironment(System::getenv);
        }

        // Package-private seam: tests supply a map-backed lookup so they never read process env.
        Builder fromEnvironment(UnaryOperator<String> env) {
            OtlpEnv.applyTo(this, env);
            return this;
        }

        public ClientTransportConfig build() {
            return new ClientTransportConfig(host, port, timeout, tls, headers, compression, retry);
        }
    }
}
