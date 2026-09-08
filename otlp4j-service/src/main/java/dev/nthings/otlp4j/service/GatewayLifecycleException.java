package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.ComponentId;

/// Identifies the component that failed, timed out, or was interrupted during a lifecycle operation.
/// Startup failures trigger rollback; flush and shutdown failures remain observable to the caller.
public final class GatewayLifecycleException extends RuntimeException {

    private final transient ComponentId component;

    /// Builds the exception for a failed component lifecycle operation.
    ///
    /// @param component the component whose lifecycle operation failed
    /// @param message   the failure explanation
    /// @param cause     the underlying failure, if any
    public GatewayLifecycleException(ComponentId component, String message, Throwable cause) {
        super(message, cause);
        this.component = component;
    }

    /// The component whose lifecycle operation failed.
    ///
    /// @return the component id
    public ComponentId component() {
        return component;
    }
}
