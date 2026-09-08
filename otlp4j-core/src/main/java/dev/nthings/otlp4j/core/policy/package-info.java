/// The neutral policy vocabulary: retry, backoff, timeout, batching, overflow, and delivery-queue
/// policies expressed as plain records and enums with no third-party type on the surface. These are
/// the public policy surface; a transport translates them internally to whatever resilience library
/// it uses, so no resilience library leaks into the public API. No circuit breaker or bulkhead policy
/// ships yet.
@NullMarked
package dev.nthings.otlp4j.core.policy;

import org.jspecify.annotations.NullMarked;
