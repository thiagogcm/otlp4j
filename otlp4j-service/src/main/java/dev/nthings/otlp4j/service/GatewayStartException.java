package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.ComponentId;

/// Thrown when a component fails to start, times out against the start deadline, or the starting
/// thread is interrupted. The gateway rolls back the components it already started before this
/// escapes, so a half-started gateway never serves. It names the component whose start failed.
public final class GatewayStartException extends RuntimeException {

    private final transient ComponentId component;

    /// Builds the exception for the component whose start failed.
    ///
    /// @param component the component that failed to start
    /// @param message   the failure explanation
    /// @param cause     the underlying failure, if any
    public GatewayStartException(ComponentId component, String message, Throwable cause) {
        super(message, cause);
        this.component = component;
    }

    /// The component whose start failed.
    ///
    /// @return the component id
    public ComponentId component() {
        return component;
    }
}
