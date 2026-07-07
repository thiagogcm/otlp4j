package dev.nthings.otlp4j.transport.otlp.internal;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.RetryInfo;
import com.google.rpc.Status;
import java.time.Duration;
import java.util.Base64;
import org.jspecify.annotations.Nullable;

/// Reads and writes the gRPC `grpc-status-details-bin` trailer, whose value is a base64-encoded
/// [com.google.rpc.Status] that may carry a [com.google.rpc.RetryInfo] in its `Any` details. This is
/// the spec-compliant server backoff hint that the legacy gRPC transport neither emitted nor consumed,
/// so it is the interop gap Phase 4 closes: the consolidated client decodes a spec-compliant
/// Collector's `RetryInfo`, and the consolidated server emits one alongside its own pushback trailer so
/// a spec-compliant client can read otlp4j's.
///
/// `com.google.rpc.Status` and `com.google.rpc.RetryInfo` arrive transitively through grpc-protobuf
/// (`proto-google-common-protos`), so no new dependency is introduced.
///
/// **Internal.** Part of the transport layer.
final class RetryInfoCodec {

    private RetryInfoCodec() {}

    /// Decodes a `grpc-status-details-bin` trailer value to the [RetryInfo] `retry_delay`, or null when
    /// the trailer is absent, not decodable, or carries no [RetryInfo] detail. The value is the raw
    /// header string as it appears on the wire (base64), matching what
    /// [com.linecorp.armeria.common.HttpHeaders#get] returns for the binary trailer.
    ///
    /// @param trailerValue the base64 `grpc-status-details-bin` value, or null when absent
    /// @return the requested retry delay, or null when there is none
    static @Nullable Duration decodeRetryDelay(@Nullable String trailerValue) {
        if (trailerValue == null) {
            return null;
        }
        byte[] raw;
        try {
            raw = Base64.getDecoder().decode(trailerValue.strip());
        } catch (IllegalArgumentException e) {
            return null;
        }
        return decodeRetryDelay(raw);
    }

    /// Decodes the serialized [com.google.rpc.Status] bytes to the [RetryInfo] `retry_delay`, or null
    /// when the bytes do not parse or carry no [RetryInfo] detail.
    ///
    /// @param statusBytes the serialized `com.google.rpc.Status`
    /// @return the requested retry delay, or null when there is none
    static @Nullable Duration decodeRetryDelay(byte[] statusBytes) {
        Status status;
        try {
            status = Status.parseFrom(statusBytes);
        } catch (InvalidProtocolBufferException e) {
            return null;
        }
        for (Any detail : status.getDetailsList()) {
            if (detail.is(RetryInfo.class)) {
                try {
                    var delay = detail.unpack(RetryInfo.class).getRetryDelay();
                    return Duration.ofSeconds(delay.getSeconds()).plusNanos(delay.getNanos());
                } catch (InvalidProtocolBufferException e) {
                    return null;
                }
            }
        }
        return null;
    }

    /// Builds a serialized [com.google.rpc.Status] carrying `code`, `message`, and a [RetryInfo] detail
    /// whose `retry_delay` is `delay`. The bytes are the `grpc-status-details-bin` trailer payload the
    /// consolidated gRPC server emits so a spec-compliant client reads the same backoff otlp4j already
    /// signals through the `grpc-retry-pushback-ms` trailer.
    ///
    /// @param code    the numeric gRPC status code the detail wraps
    /// @param message the status message
    /// @param delay   the requested retry delay
    /// @return the serialized `com.google.rpc.Status`
    static byte[] encodeRetryInfo(int code, String message, Duration delay) {
        var retryDelay = com.google.protobuf.Duration.newBuilder()
                .setSeconds(delay.getSeconds())
                .setNanos(delay.getNano())
                .build();
        var retryInfo = RetryInfo.newBuilder().setRetryDelay(retryDelay).build();
        return Status.newBuilder()
                .setCode(code)
                .setMessage(message)
                .addDetails(Any.pack(retryInfo))
                .build()
                .toByteArray();
    }
}
