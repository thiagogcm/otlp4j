package dev.nthings.otlp4j.transport;

/// Validation for a TCP port, shared by the OTLP exporter and receiver configs so both reject an
/// out-of-range port with one message. Mirrors the [Headers] validation idiom.
final class Ports {

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
