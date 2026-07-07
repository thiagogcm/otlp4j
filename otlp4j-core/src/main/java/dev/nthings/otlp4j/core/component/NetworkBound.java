package dev.nthings.otlp4j.core.component;

/// A transport-runtime capability: a component that binds a network socket and can report its bound
/// port. This is not extended by `Component` and not implemented by every component; a receiver that
/// binds a socket implements it alongside `Receiver`, so a caller holding a live component can
/// downcast and read the OS-assigned bound port after start. This is how a caller reads a
/// graph-booted receiver's ephemeral port, the graph-runtime equivalent of the legacy
/// `Receiver.port()`.
public interface NetworkBound {

    /// The bound port, valid only after the component has started. For a receiver bound to an
    /// ephemeral port (port `0`), this returns the OS-assigned port.
    ///
    /// @return the bound port
    int port();
}
