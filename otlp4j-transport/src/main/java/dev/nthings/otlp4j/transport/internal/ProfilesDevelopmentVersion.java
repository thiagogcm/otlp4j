package dev.nthings.otlp4j.transport.internal;

import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.List;
import org.jspecify.annotations.Nullable;

/// The receiving side of the OTLP Profiles development version: request metadata that names the schema
/// of a `v1development` request, so that a server can refuse one it cannot read before deserializing
/// it. A request without the metadata is version 1, the only version this transport speaks, which is
/// also why the exporter does not send it.
final class ProfilesDevelopmentVersion {

    /// The OTLP/HTTP header and the OTLP/gRPC metadata key.
    static final String HEADER = "otlp-profiles-development-version";

    private static final String SUPPORTED = "1";

    private ProfilesDevelopmentVersion() {}

    /// Why a request carrying `values` must be refused, or `null` when it may be read. Every refusal
    /// answers alike, so a malformed value, a repeated one, and an unsupported version need no telling
    /// apart.
    ///
    /// @param values every value of the metadata on the request, empty when it is absent
    /// @return the refusal message, or `null` to accept the request
    static @Nullable String rejection(List<String> values) {
        return values.isEmpty() || values.equals(List.of(SUPPORTED))
                ? null
                : "this server reads " + HEADER + " " + SUPPORTED + ", not " + values;
    }

    /// Refuses with `400` what [#rejection] refuses, before the request body is read.
    static HttpService guard(HttpService next) {
        return next.decorate((delegate, ctx, req) -> {
            var rejection = rejection(req.headers().getAll(HEADER));
            return rejection == null
                    ? delegate.serve(ctx, req)
                    : HttpProtocolMapping.text(HttpStatus.BAD_REQUEST, rejection);
        });
    }

    /// Closes a gRPC call that [#rejection] refuses with `INVALID_ARGUMENT`. Armeria releases the
    /// message of a closed call undecoded.
    static ServerInterceptor grpcInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                var rejection = rejection(ServiceRequestContext.current().request().headers().getAll(HEADER));
                if (rejection == null) {
                    return next.startCall(call, headers);
                }
                call.close(Status.INVALID_ARGUMENT.withDescription(rejection), new Metadata());
                return new ServerCall.Listener<>() {};
            }
        };
    }
}
