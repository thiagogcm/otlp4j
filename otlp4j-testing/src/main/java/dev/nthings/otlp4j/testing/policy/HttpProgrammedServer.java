package dev.nthings.otlp4j.testing.policy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/// The OTLP/HTTP variant of [ProgrammedOtlpServer], serving the four `/v1/<signal>` POST endpoints over
/// the JDK `HttpServer`. It maps each neutral [ProgrammedResponse] to the HTTP wire the client already
/// reads: a full success is a 200 with an empty export response, a retryable rejection is a 503, a
/// throttle carries a `Retry-After` header (numeric seconds, or an absolute RFC 1123 date for
/// [ProgrammedResponse.ThrottleDate]), a permanent rejection is a 400, and a hang sleeps before
/// answering so the client's per-attempt request timeout fires.
final class HttpProgrammedServer extends ProgrammedOtlpServer {

    private static final byte[] OK_BODY = ExportTraceServiceResponse.getDefaultInstance().toByteArray();

    private final HttpServer server;
    private final ExecutorService executor;

    private HttpProgrammedServer(List<ProgrammedResponse> script, HttpServer server, ExecutorService executor) {
        super(script);
        this.server = server;
        this.executor = executor;
    }

    static HttpProgrammedServer started(List<ProgrammedResponse> script) {
        try {
            var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            // A controlled executor so a hung handler can be interrupted at close, keeping teardown
            // bounded instead of waiting out the hang.
            var executor = Executors.newCachedThreadPool();
            var instance = new HttpProgrammedServer(script, server, executor);
            server.createContext("/", instance::handle);
            server.setExecutor(executor);
            server.start();
            return instance;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to start the programmed OTLP/HTTP server", e);
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            // Drain the request body so the connection is reusable and the attempt is fully received.
            exchange.getRequestBody().readAllBytes();
            var response = recordAndNext();
            switch (response) {
                case ProgrammedResponse.Ok ignored -> respond(exchange, 200, OK_BODY);
                case ProgrammedResponse.Unavailable ignored -> respond(exchange, 503, new byte[0]);
                case ProgrammedResponse.Throttle throttle -> {
                    exchange.getResponseHeaders().set("Retry-After", Long.toString(ceilSeconds(throttle.retryAfter())));
                    respond(exchange, 503, new byte[0]);
                }
                case ProgrammedResponse.ThrottleDate throttle -> {
                    exchange.getResponseHeaders().set("Retry-After", httpDate(throttle.retryAfter()));
                    respond(exchange, 503, new byte[0]);
                }
                case ProgrammedResponse.ThrottleNoRetry ignored -> respond(exchange, 400, new byte[0]);
                case ProgrammedResponse.RetryInfo retryInfo -> {
                    // HTTP has no RetryInfo detail; the cross-protocol analog is a Retry-After header,
                    // which the HTTP client folds like any other throttle.
                    exchange.getResponseHeaders().set("Retry-After", Long.toString(ceilSeconds(retryInfo.retryAfter())));
                    respond(exchange, 503, new byte[0]);
                }
                case ProgrammedResponse.Permanent ignored -> respond(exchange, 400, new byte[0]);
                case ProgrammedResponse.Hang hang -> {
                    sleep(hang.duration());
                    respond(exchange, 200, OK_BODY);
                }
            }
        } catch (RuntimeException e) {
            // A hung handler interrupted at close, or the client having already abandoned the attempt:
            // close the exchange quietly so teardown is not blocked and no stack trace is printed.
            exchange.close();
        }
    }

    private static void respond(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            exchange.getResponseBody().write(body);
        }
    }

    /// Rounds a duration up to whole seconds, floored at one, so a sub-second throttle still yields a
    /// positive `Retry-After` on HTTP's one-second header resolution.
    private static long ceilSeconds(Duration d) {
        return Math.max(1, (long) Math.ceil(d.toMillis() / 1000.0));
    }

    /// An absolute RFC 1123 `Retry-After` value comfortably in the future, so the client's date parse
    /// yields a positive delay despite the header's one-second resolution. The extra second guards
    /// against truncation of a sub-second requested delay to the current second.
    private static String httpDate(Duration retryAfter) {
        var at = ZonedDateTime.now().plus(retryAfter).plusSeconds(1);
        return at.format(DateTimeFormatter.RFC_1123_DATE_TIME);
    }

    private static void sleep(Duration d) {
        try {
            Thread.sleep(Math.max(0, d.toMillis()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted while holding a programmed attempt open", e);
        }
    }

    @Override
    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }
}
