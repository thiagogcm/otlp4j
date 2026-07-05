package dev.nthings.otlp4j.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Package-private helpers backing the [Lifecycle] default methods. Holds the slf4j logger so no
/// logger field leaks onto the public [Lifecycle] interface.
final class LifecycleSupport {

    private static final Logger log = LoggerFactory.getLogger(Lifecycle.class);

    private LifecycleSupport() {
    }

    /// Logs, at WARN, that `owner`'s best-effort drain during [Lifecycle#close] did not complete
    /// cleanly. `close()` swallows the failure (a throw during teardown would mask the primary
    /// exception in a try-with-resources block), so this log is the only surfacing.
    ///
    /// @param owner the resource whose close drain failed
    /// @param error the drain failure (a `CompletionException` or `CancellationException`)
    static void warnCloseDrainFailed(Lifecycle owner, Throwable error) {
        var cause = error.getCause() != null ? error.getCause() : error;
        log.warn("{} did not cleanly drain on close: {}", owner.getClass().getName(), cause.toString());
    }
}
