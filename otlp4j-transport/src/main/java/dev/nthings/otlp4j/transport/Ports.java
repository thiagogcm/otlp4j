package dev.nthings.otlp4j.transport;

/// The conventional OTLP ports and the validation for a TCP port, shared by the OTLP exporter and
/// receiver configs and factories so both default to one port per protocol and reject an
/// out-of-range port with one message. Mirrors the [Headers] validation idiom.
final class Ports {

    /// The conventional OTLP/gRPC port.
    static final int GRPC = 4317;

    /// The conventional OTLP/HTTP port.
    static final int HTTP = 4318;

    private Ports() {}

    /// Validates that `port` is a legal bind/connect port (`0`-`65535`; `0` selects an ephemeral port)
    /// and returns it for fluent use.
    ///
    /// @param port the port to validate
    /// @return the validated `port`
    /// @throws IllegalArgumentException when the port is outside `0`-`65535`
    static int requireValid(int port) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        return port;
    }
}
