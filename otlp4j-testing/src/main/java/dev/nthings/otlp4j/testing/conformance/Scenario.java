package dev.nthings.otlp4j.testing.conformance;

import java.time.Duration;

/// One conformance case's server-side configuration, expressed in neutral terms.
///
/// A scenario says which signal to drive, what outcome to program the consumer with, whether to
/// hold the first request open for an admission test, the admission caps and throttle hint, the
/// decoded-message size cap, and the TLS posture. The seam implementation translates it to the
/// current runtime's builders; the suite constructs one per test through [#builder(ConformanceSignal)].
///
/// @param signal the signal whose consumer and endpoint the case exercises
/// @param outcome the result the consumer is programmed to produce
/// @param holdFirstRequest whether the first consumer invocation is held open until the test
///     releases it, which is how the admission cases pin a permit while a second request arrives
/// @param maxInFlightRequests the admission cap on concurrent in-flight requests
/// @param maxInFlightBytes the admission cap on concurrent in-flight decoded bytes
/// @param throttleRetryAfter the backoff hint the gateway advertises when admission refuses a
///     request, or null to advertise none
/// @param maxInboundMessageSizeBytes the cap on a single decoded message, above which the gateway
///     rejects the payload as oversized
/// @param tls the TLS posture the gateway and wire client take
public record Scenario(
        ConformanceSignal signal,
        ProgrammedOutcome outcome,
        boolean holdFirstRequest,
        int maxInFlightRequests,
        long maxInFlightBytes,
        Duration throttleRetryAfter,
        int maxInboundMessageSizeBytes,
        TlsMode tls) {

    /// Default admission request cap, chosen loose so ordinary cases never trip admission.
    public static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 1024;

    /// Default admission byte cap, chosen loose so ordinary cases never trip admission.
    public static final long DEFAULT_MAX_IN_FLIGHT_BYTES = 64L * 1024 * 1024;

    /// Default decoded-message cap, generous enough that only the oversized case trips it.
    public static final int DEFAULT_MAX_INBOUND_MESSAGE_SIZE_BYTES = 4 * 1024 * 1024;

    /// Starts a builder for the given signal, with all other fields defaulted to the values an
    /// ordinary accepted case wants: an [ProgrammedOutcome.Accepted] outcome, no held request,
    /// loose admission caps, no throttle hint, the default size cap, and plaintext.
    ///
    /// @param signal the signal the case drives
    /// @return a fresh builder
    public static Builder builder(ConformanceSignal signal) {
        return new Builder(signal);
    }

    /// A readable, per-test builder for [Scenario]. Every setter returns this builder.
    public static final class Builder {

        private final ConformanceSignal signal;
        private ProgrammedOutcome outcome = new ProgrammedOutcome.Accepted();
        private boolean holdFirstRequest;
        private int maxInFlightRequests = DEFAULT_MAX_IN_FLIGHT_REQUESTS;
        private long maxInFlightBytes = DEFAULT_MAX_IN_FLIGHT_BYTES;
        private Duration throttleRetryAfter;
        private int maxInboundMessageSizeBytes = DEFAULT_MAX_INBOUND_MESSAGE_SIZE_BYTES;
        private TlsMode tls = TlsMode.NONE;

        private Builder(ConformanceSignal signal) {
            this.signal = signal;
        }

        /// Programs the consumer's outcome.
        ///
        /// @param value the outcome to produce
        /// @return this builder
        public Builder outcome(ProgrammedOutcome value) {
            this.outcome = value;
            return this;
        }

        /// Holds the first consumer invocation open until [GatewayUnderTest#releaseHeldRequest].
        ///
        /// @param value whether to hold the first request
        /// @return this builder
        public Builder holdFirstRequest(boolean value) {
            this.holdFirstRequest = value;
            return this;
        }

        /// Sets the admission cap on concurrent in-flight requests.
        ///
        /// @param value the request cap
        /// @return this builder
        public Builder maxInFlightRequests(int value) {
            this.maxInFlightRequests = value;
            return this;
        }

        /// Sets the admission cap on concurrent in-flight decoded bytes.
        ///
        /// @param value the byte cap
        /// @return this builder
        public Builder maxInFlightBytes(long value) {
            this.maxInFlightBytes = value;
            return this;
        }

        /// Sets the backoff hint the gateway advertises when admission refuses a request.
        ///
        /// @param value the retry-after hint, or null for none
        /// @return this builder
        public Builder throttleRetryAfter(Duration value) {
            this.throttleRetryAfter = value;
            return this;
        }

        /// Sets the cap on a single decoded message.
        ///
        /// @param value the size cap in bytes
        /// @return this builder
        public Builder maxInboundMessageSizeBytes(int value) {
            this.maxInboundMessageSizeBytes = value;
            return this;
        }

        /// Sets the TLS posture.
        ///
        /// @param value the TLS mode
        /// @return this builder
        public Builder tls(TlsMode value) {
            this.tls = value;
            return this;
        }

        /// Builds the immutable scenario.
        ///
        /// @return the scenario
        public Scenario build() {
            return new Scenario(
                    signal,
                    outcome,
                    holdFirstRequest,
                    maxInFlightRequests,
                    maxInFlightBytes,
                    throttleRetryAfter,
                    maxInboundMessageSizeBytes,
                    tls);
        }
    }
}
