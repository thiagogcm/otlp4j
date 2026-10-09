package dev.nthings.otlp4j.transport;

import dev.nthings.otlp4j.service.GatewayRuntime;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/// Releases what the wire tests of one class start. Register it as a static extension field.
///
/// A client or exporter is closed after the test that created it. Receivers and graph runtimes are
/// stopped together when the class finishes. One graceful receiver stop takes about three seconds
/// whatever the test did: this project's one-second quiet period plus the two seconds Netty's accept
/// loop takes to shut down inside Armeria. Stopping after every test therefore costs three seconds per
/// test, while stopping together costs them once per class. Each test still gets its own receiver on
/// its own ephemeral port, and a test that asserts on a shutdown still performs it itself.
final class WireCleanup implements AfterEachCallback, AfterAllCallback {

    private static final Duration STOP_BUDGET = Duration.ofSeconds(30);

    private final List<AutoCloseable> clients = new CopyOnWriteArrayList<>();
    private final List<Function<Duration, CompletionStage<Void>>> servers = new CopyOnWriteArrayList<>();

    /// Closes `client` after the current test.
    <T extends AutoCloseable> T client(T client) {
        clients.add(client);
        return client;
    }

    /// Stops `receiver` when the test class finishes.
    OtlpReceiver receiver(OtlpReceiver receiver) {
        servers.add(receiver::shutdown);
        return receiver;
    }

    /// Stops `runtime` when the test class finishes.
    GatewayRuntime runtime(GatewayRuntime runtime) {
        servers.add(runtime::shutdown);
        return runtime;
    }

    @Override
    public void afterEach(ExtensionContext context) {
        for (var client : clients) {
            try {
                client.close();
            } catch (Exception ignored) {
                // keep tearing down
            }
        }
        clients.clear();
    }

    @Override
    public void afterAll(ExtensionContext context) {
        var stops = new ArrayList<CompletableFuture<Void>>();
        for (var server : servers) {
            try {
                stops.add(server.apply(STOP_BUDGET).toCompletableFuture());
            } catch (RuntimeException ignored) {
                // keep tearing down
            }
        }
        servers.clear();
        for (var stop : stops) {
            try {
                stop.get(STOP_BUDGET.toSeconds(), TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // keep tearing down
            }
        }
    }
}
