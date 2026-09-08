package dev.nthings.otlp4j.transport.internal;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/** gRPC request metadata carried through the server Context. */
final class RequestMetadata {

    private static final Context.Key<Metadata> KEY = Context.keyWithDefault(
            "otlp4j.request-metadata", new Metadata());

    private RequestMetadata() {}

    static Metadata current() {
        return KEY.get();
    }

    static ServerInterceptor interceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                var context = Context.current().withValue(KEY, headers);
                return Contexts.interceptCall(context, call, headers, next);
            }
        };
    }
}
