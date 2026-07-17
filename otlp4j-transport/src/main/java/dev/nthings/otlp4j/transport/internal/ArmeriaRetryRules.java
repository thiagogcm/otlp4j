package dev.nthings.otlp4j.transport.internal;

import com.linecorp.armeria.client.ClientRequestContext;
import com.linecorp.armeria.client.HttpClient;
import com.linecorp.armeria.client.retry.Backoff;
import com.linecorp.armeria.client.retry.RetryConfig;
import com.linecorp.armeria.client.retry.RetryDecision;
import com.linecorp.armeria.client.retry.RetryRule;
import com.linecorp.armeria.client.retry.RetryingClient;
import com.linecorp.armeria.common.HttpHeaders;
import com.linecorp.armeria.common.ResponseHeaders;
import com.linecorp.armeria.common.logging.RequestLog;
import com.linecorp.armeria.common.logging.RequestLogProperty;
import dev.nthings.otlp4j.core.policy.BackoffPolicy;
import dev.nthings.otlp4j.core.policy.RetryPolicy;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/// The single translation of the neutral [RetryPolicy] / [BackoffPolicy] and per-attempt / total
/// deadlines onto Armeria's retrying client, serving both OTLP protocols from one runtime. It produces
/// the [RetryingClient] decorator each protocol's client installs, so retry, backoff, and the server
/// throttling hints are delegated to Armeria rather than a hand-rolled loop.
///
/// The backoff mirrors the neutral [BackoffPolicy]: an exponential schedule with the policy's initial
/// delay, growth multiplier, jitter, and cap. The attempt cap and per-attempt deadline are set on the
/// retry config; the total deadline is the client-level response timeout the caller sets separately.
///
/// The retry decision folds the server throttling hints in a fixed precedence, evaluated per attempt
/// against the response headers and trailers:
///
/// 1. A negative gRPC `grpc-retry-pushback-ms` trailer is a stop-retry directive and wins outright over
///    every other signal: the delivery ends at a single attempt. This holds even when the policy
///    disables respecting server hints, matching the legacy gRPC client.
/// 2. Otherwise, when the response is retryable and the policy respects server hints, a
///    `RetryInfo` in the `grpc-status-details-bin` trailer wins over a positive `grpc-retry-pushback-ms`
///    trailer, which in turn wins over the policy's own exponential backoff.
/// 3. On OTLP/HTTP a retryable response's `Retry-After` header is parsed in the HTTP rule, in both the
///    numeric delta-seconds form and the RFC 1123 absolute-date form, and folded as the next backoff
///    when the policy respects server hints; it is ignored when it does not. Armeria's own
///    `useRetryAfter` is not used, because it honours only the delta-seconds form and misses the
///    absolute-date form the OTLP/HTTP spec also allows. gRPC carries no `Retry-After` header, so that
///    path only reaches the trailer hints above.
///
/// **Internal.** Part of the transport layer.
final class ArmeriaRetryRules {

    /// The `grpc-status` trailer (or trailers-only header) carrying the numeric gRPC status code.
    static final String GRPC_STATUS = "grpc-status";

    /// The `grpc-retry-pushback-ms` trailer: a server backoff hint in milliseconds; negative means stop.
    static final String PUSHBACK_MS = "grpc-retry-pushback-ms";

    /// The `grpc-status-details-bin` trailer: a base64 `com.google.rpc.Status` that may carry `RetryInfo`.
    static final String STATUS_DETAILS_BIN = "grpc-status-details-bin";

    /// The OTLP/HTTP `Retry-After` response header; case-insensitive on lookup.
    static final String RETRY_AFTER = "retry-after";

    /// gRPC status codes the client retries, mirroring the legacy gRPC transport exactly: `CANCELLED`,
    /// `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `ABORTED`, `OUT_OF_RANGE`, `UNAVAILABLE`, `DATA_LOSS`.
    private static final Set<Integer> RETRYABLE_GRPC = Set.of(1, 4, 8, 10, 11, 14, 15);

    /// HTTP statuses the client retries, mirroring the legacy OTLP/HTTP transport: 408, 429, 502, 503, 504.
    private static final Set<Integer> RETRYABLE_HTTP = Set.of(408, 429, 502, 503, 504);

    private ArmeriaRetryRules() {}

    /// Builds the [Backoff] equivalent of the neutral [BackoffPolicy]: an exponential schedule from the
    /// policy's initial delay to its cap, growing by its multiplier, perturbed by its jitter fraction. A
    /// multiplier of exactly `1.0` is a constant schedule (Armeria's exponential requires a multiplier
    /// strictly greater than one), so it maps to a fixed backoff at the initial delay.
    ///
    /// @param policy the neutral backoff policy
    /// @return the equivalent Armeria backoff
    static Backoff backoff(BackoffPolicy policy) {
        var base = Math.max(1, policy.initial().toMillis());
        var max = Math.max(base, policy.max().toMillis());
        var backoff = policy.multiplier() > 1.0
                ? Backoff.exponential(base, max, policy.multiplier())
                : Backoff.fixed(base);
        return backoff.withJitter(policy.jitter());
    }

    /// Builds the [RetryingClient] decorator for `protocol` from the neutral retry policy and the
    /// per-attempt deadline. The attempt cap and per-attempt response timeout live on the retry config;
    /// the total deadline is the client-level response timeout set by the caller. The HTTP `Retry-After`
    /// header is parsed and folded inside the HTTP rule (both the numeric and the RFC 1123 date forms),
    /// so Armeria's own `useRetryAfter` is left off; gRPC folds its trailer hints in the gRPC rule.
    ///
    /// @param protocol   the wire protocol the client speaks
    /// @param policy     the neutral retry policy
    /// @param perAttempt the per-attempt deadline
    /// @return the retrying-client decorator to install on the protocol client
    static Function<? super HttpClient, RetryingClient> decorator(
            Protocol protocol, RetryPolicy policy, Duration perAttempt) {
        var backoff = backoff(policy.backoff());
        var respectHints = policy.respectServerRetryAfter();
        RetryRule rule = switch (protocol) {
            case GRPC -> new GrpcRetryRule(backoff, respectHints);
            case HTTP -> new HttpRetryRule(backoff, respectHints);
        };
        var config = RetryConfig.builder(rule)
                .maxTotalAttempts(policy.maxAttempts())
                .responseTimeoutMillisForEachAttempt(Math.max(0, perAttempt.toMillis()))
                .build();
        return RetryingClient.builder(config).newDecorator();
    }

    // --- pure decision logic (unit-pinned) -----------------------------------------------------

    /// What the retry rule decided for one attempt: whether to retry and with what delay.
    enum Kind {
        /// The response is not retryable; stop.
        NO_RETRY,
        /// A server stop-retry directive (negative pushback); stop.
        STOP,
        /// Retry with the policy's own exponential backoff.
        RETRY_DEFAULT,
        /// Retry after the server-requested delay.
        RETRY_DELAY
    }

    /// A retry decision: its [Kind] and, for [Kind#RETRY_DELAY], the server-requested delay.
    ///
    /// @param kind  the decision kind
    /// @param delay the requested delay for [Kind#RETRY_DELAY], else null
    record Decision(Kind kind, @Nullable Duration delay) {

        static Decision noRetry() {
            return new Decision(Kind.NO_RETRY, null);
        }

        static Decision stop() {
            return new Decision(Kind.STOP, null);
        }

        static Decision retryDefault() {
            return new Decision(Kind.RETRY_DEFAULT, null);
        }

        static Decision retryAfter(Duration delay) {
            return new Decision(Kind.RETRY_DELAY, delay);
        }
    }

    /// The gRPC retry decision from the parsed per-attempt signals, in the documented precedence.
    ///
    /// @param grpcStatus       the numeric gRPC status, or null when the attempt failed at the transport
    /// @param pushbackMs       the `grpc-retry-pushback-ms` value, or null when absent
    /// @param retryInfoDelay   the `RetryInfo` retry delay, or null when absent
    /// @param transportFailure true when the attempt failed with an exception and no gRPC status
    /// @param respectHints     whether the policy respects positive server backoff hints
    /// @return the decision
    static Decision decideGrpc(
            @Nullable Integer grpcStatus,
            @Nullable Long pushbackMs,
            @Nullable Duration retryInfoDelay,
            boolean transportFailure,
            boolean respectHints) {
        if (transportFailure) {
            // A connection reset or a per-attempt deadline: retry on the policy's own backoff.
            return Decision.retryDefault();
        }
        // A stop-retry directive overrides every other signal and holds even when hints are disabled.
        if (pushbackMs != null && pushbackMs < 0) {
            return Decision.stop();
        }
        var retryable = grpcStatus != null && RETRYABLE_GRPC.contains(grpcStatus);
        if (!retryable) {
            return Decision.noRetry();
        }
        if (respectHints) {
            if (retryInfoDelay != null) {
                return Decision.retryAfter(retryInfoDelay);
            }
            if (pushbackMs != null && pushbackMs > 0) {
                return Decision.retryAfter(Duration.ofMillis(pushbackMs));
            }
        }
        return Decision.retryDefault();
    }

    /// The HTTP retry decision from the status alone. The `Retry-After` header is not read here: the
    /// [HttpRetryRule] parses and folds it onto a [Kind#RETRY_DEFAULT] result when the policy respects
    /// server hints, so this pure function stays a plain status-to-retryability mapping.
    ///
    /// @param httpStatus       the response status code, or null when the attempt failed at the transport
    /// @param transportFailure true when the attempt failed with an exception and no response
    /// @return the decision
    static Decision decideHttp(@Nullable Integer httpStatus, boolean transportFailure) {
        if (transportFailure) {
            return Decision.retryDefault();
        }
        var retryable = httpStatus != null && RETRYABLE_HTTP.contains(httpStatus);
        return retryable ? Decision.retryDefault() : Decision.noRetry();
    }

    /// Maps a neutral [Decision] onto an Armeria [RetryDecision] against `backoff`.
    private static RetryDecision toArmeria(Decision decision, Backoff backoff) {
        return switch (decision.kind()) {
            case NO_RETRY, STOP -> RetryDecision.noRetry();
            case RETRY_DEFAULT -> RetryDecision.retry(backoff);
            case RETRY_DELAY -> RetryDecision.retry(Backoff.fixed(Math.max(0, decision.delay().toMillis())));
        };
    }

    /// Parses a decimal `long` header value, or null when absent or malformed.
    private static @Nullable Long parseLong(@Nullable String value) {
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value.strip());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /// Parses an OTLP/HTTP `Retry-After` header value into a positive delay from now, in both forms the
    /// spec allows: a numeric delta-seconds count, and an RFC 1123 absolute date. Returns null when the
    /// header is absent, unparseable, or resolves to a non-positive delay, so the caller falls back to
    /// the policy's own backoff.
    ///
    /// @param value the raw `Retry-After` header value, or null when absent
    /// @return the positive delay from now, or null
    static @Nullable Duration parseRetryAfter(@Nullable String value) {
        if (value == null) {
            return null;
        }
        var trimmed = value.strip();
        var seconds = parseLong(trimmed);
        if (seconds != null) {
            return seconds > 0 ? Duration.ofSeconds(seconds) : null;
        }
        try {
            var at = ZonedDateTime.parse(trimmed, DateTimeFormatter.RFC_1123_DATE_TIME);
            var delayMillis = at.toInstant().toEpochMilli() - System.currentTimeMillis();
            return delayMillis > 0 ? Duration.ofMillis(delayMillis) : null;
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    /// Reads `name` from the trailers first, then the response headers (a trailers-only gRPC error
    /// carries its status metadata in the headers frame, so both must be consulted).
    private static @Nullable String signal(
            @Nullable ResponseHeaders headers, HttpHeaders trailers, String name) {
        var value = trailers.get(name);
        if (value == null && headers != null) {
            value = headers.get(name);
        }
        return value;
    }

    private static CompletionStage<RetryDecision> completed(RetryDecision decision) {
        return CompletableFuture.completedFuture(decision);
    }

    // --- Armeria rule adapters -----------------------------------------------------------------

    /// The gRPC retry rule: waits for the response trailers, extracts the gRPC status, pushback, and
    /// `RetryInfo` from the headers and trailers, and applies the precedence in [#decideGrpc].
    private static final class GrpcRetryRule implements RetryRule {

        private final Backoff backoff;
        private final boolean respectHints;

        GrpcRetryRule(Backoff backoff, boolean respectHints) {
            this.backoff = backoff;
            this.respectHints = respectHints;
        }

        @Override
        public boolean requiresResponseTrailers() {
            return true;
        }

        @Override
        public CompletionStage<RetryDecision> shouldRetry(
                ClientRequestContext ctx, @Nullable Throwable cause) {
            if (cause != null) {
                return completed(toArmeria(
                        decideGrpc(null, null, null, true, respectHints), backoff));
            }
            RequestLog log = ctx.log().getIfAvailable(
                    RequestLogProperty.RESPONSE_HEADERS, RequestLogProperty.RESPONSE_TRAILERS);
            HttpHeaders trailers = log != null ? log.responseTrailers() : HttpHeaders.of();
            ResponseHeaders headers = log != null ? log.responseHeaders() : null;
            Integer status = intOrNull(signal(headers, trailers, GRPC_STATUS));
            Long pushback = parseLong(signal(headers, trailers, PUSHBACK_MS));
            Duration retryInfo = RetryInfoCodec.decodeRetryDelay(signal(headers, trailers, STATUS_DETAILS_BIN));
            var decision = decideGrpc(status, pushback, retryInfo, false, respectHints);
            return completed(toArmeria(decision, backoff));
        }

        private static @Nullable Integer intOrNull(@Nullable String value) {
            var parsed = parseLong(value);
            return parsed == null ? null : Math.toIntExact(parsed);
        }
    }

    /// The HTTP retry rule: retries the retryable statuses, and when the policy respects server hints
    /// folds a retryable response's `Retry-After` header (numeric or RFC 1123 date) as the next backoff.
    private static final class HttpRetryRule implements RetryRule {

        private final Backoff backoff;
        private final boolean respectHints;

        HttpRetryRule(Backoff backoff, boolean respectHints) {
            this.backoff = backoff;
            this.respectHints = respectHints;
        }

        @Override
        public CompletionStage<RetryDecision> shouldRetry(
                ClientRequestContext ctx, @Nullable Throwable cause) {
            if (cause != null) {
                return completed(toArmeria(decideHttp(null, true), backoff));
            }
            RequestLog log = ctx.log().getIfAvailable(RequestLogProperty.RESPONSE_HEADERS);
            Integer status = null;
            String retryAfter = null;
            if (log != null) {
                var responseHeaders = log.responseHeaders();
                status = responseHeaders.status().code();
                retryAfter = responseHeaders.get(RETRY_AFTER);
            }
            var decision = decideHttp(status, false);
            if (decision.kind() == Kind.RETRY_DEFAULT && respectHints) {
                var delay = parseRetryAfter(retryAfter);
                if (delay != null) {
                    decision = Decision.retryAfter(delay);
                }
            }
            return completed(toArmeria(decision, backoff));
        }
    }
}
