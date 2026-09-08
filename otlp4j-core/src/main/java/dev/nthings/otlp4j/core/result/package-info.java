/// The delivery result taxonomy: the outcome of feeding one telemetry payload to a consumer.
/// `DeliveryResult` is the sealed accepted / partial / rejected trichotomy. `Disposition` is the
/// retryable-versus-permanent axis, `DeliveryReason`
/// names the cause as a reason code, and `Provenance` records which component produced the result so
/// fan-out can preserve it. The whole-type taxonomy is permanent, retryable, and downstream; the
/// downstream marker is a single boolean on a rejection, orthogonal to retryability.
@NullMarked
package dev.nthings.otlp4j.core.result;

import org.jspecify.annotations.NullMarked;
