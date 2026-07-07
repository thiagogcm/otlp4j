package dev.nthings.otlp4j.codec;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.model.LogsData;
import dev.nthings.otlp4j.model.MetricsData;
import dev.nthings.otlp4j.model.ProfilesData;
import dev.nthings.otlp4j.model.Signal;
import dev.nthings.otlp4j.model.TracesData;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/// Binds one [Signal] to its proto marshalling: decode, encode, delivery-result interpretation,
/// delivery-result response building, and byte-level parse. Aggregates the existing per-signal
/// mapper statics behind a [Signal]-keyed value so transports dispatch once instead of four times.
///
/// Internal. Proto types appear here; `otlp4j-proto` is qualified-exported only to codec and the two
/// transports, so this surface stays usable only by bundled transports. Never referenced from
/// `otlp4j-model`.
///
/// @param <T>    the domain batch type
/// @param <REQ>  the signal's proto export-request message
/// @param <RESP> the signal's proto export-response message
public final class SignalCodec<T, REQ extends MessageLite, RESP extends MessageLite> {

    /// Trace marshalling.
    public static final SignalCodec<TracesData, ExportTraceServiceRequest, ExportTraceServiceResponse> TRACES =
            new SignalCodec<>(Signal.TRACES, SignalDescriptor.TRACES,
                    TraceMapper::toDomain, TraceMapper::toProto,
                    TraceMapper::deliveryResult, DeliveryResultResponses::traces,
                    ExportTraceServiceRequest::parseFrom, ExportTraceServiceResponse::parseFrom);

    /// Metrics marshalling.
    public static final SignalCodec<MetricsData, ExportMetricsServiceRequest, ExportMetricsServiceResponse> METRICS =
            new SignalCodec<>(Signal.METRICS, SignalDescriptor.METRICS,
                    MetricsMapper::toDomain, MetricsMapper::toProto,
                    MetricsMapper::deliveryResult, DeliveryResultResponses::metrics,
                    ExportMetricsServiceRequest::parseFrom, ExportMetricsServiceResponse::parseFrom);

    /// Logs marshalling.
    public static final SignalCodec<LogsData, ExportLogsServiceRequest, ExportLogsServiceResponse> LOGS =
            new SignalCodec<>(Signal.LOGS, SignalDescriptor.LOGS,
                    LogsMapper::toDomain, LogsMapper::toProto,
                    LogsMapper::deliveryResult, DeliveryResultResponses::logs,
                    ExportLogsServiceRequest::parseFrom, ExportLogsServiceResponse::parseFrom);

    /// Profiles marshalling (development signal).
    public static final SignalCodec<ProfilesData, ExportProfilesServiceRequest, ExportProfilesServiceResponse> PROFILES =
            new SignalCodec<>(Signal.PROFILES, SignalDescriptor.PROFILES,
                    ProfilesMapper::toDomain, ProfilesMapper::toProto,
                    ProfilesMapper::deliveryResult, DeliveryResultResponses::profiles,
                    ExportProfilesServiceRequest::parseFrom, ExportProfilesServiceResponse::parseFrom);

    private static final List<SignalCodec<?, ?, ?>> VALUES = List.of(TRACES, METRICS, LOGS, PROFILES);

    private final Signal<T> signal;
    private final SignalDescriptor<T> descriptor;
    private final Function<REQ, T> toDomain;
    private final Function<T, REQ> toProto;
    private final BiFunction<RESP, Provenance, DeliveryResult> deliveryResult;
    private final Function<DeliveryResult, RESP> deliveryResponse;
    private final Parse<REQ> parseRequest;
    private final Parse<RESP> parseResponse;

    private SignalCodec(Signal<T> signal, SignalDescriptor<T> descriptor,
                        Function<REQ, T> toDomain, Function<T, REQ> toProto,
                        BiFunction<RESP, Provenance, DeliveryResult> deliveryResult,
                        Function<DeliveryResult, RESP> deliveryResponse,
                        Parse<REQ> parseRequest, Parse<RESP> parseResponse) {
        this.signal = signal;
        this.descriptor = descriptor;
        this.toDomain = toDomain;
        this.toProto = toProto;
        this.deliveryResult = deliveryResult;
        this.deliveryResponse = deliveryResponse;
        this.parseRequest = parseRequest;
        this.parseResponse = parseResponse;
    }

    /// The signal this codec marshals.
    public Signal<T> signal() {
        return signal;
    }

    /// The core-side signal descriptor this codec marshals, the [DeliveryResult] world's counterpart
    /// of [#signal()].
    ///
    /// @return the signal descriptor
    public SignalDescriptor<T> descriptor() {
        return descriptor;
    }

    /// Decodes a proto export request into its domain batch.
    public T toDomain(REQ request) {
        return toDomain.apply(request);
    }

    /// Encodes a domain batch into its proto export request.
    public REQ toProto(T batch) {
        return toProto.apply(batch);
    }

    /// Interprets a proto export response as a [DeliveryResult] attributed to `provenance`.
    ///
    /// @param response   the proto export response
    /// @param provenance the provenance to attribute the result to
    /// @return the delivery result
    public DeliveryResult deliveryResult(RESP response, Provenance provenance) {
        return deliveryResult.apply(response, provenance);
    }

    /// Builds the proto export response for a [DeliveryResult].
    ///
    /// @param r the delivery result
    /// @return the proto export response
    public RESP deliveryResponse(DeliveryResult r) {
        return deliveryResponse.apply(r);
    }

    /// Parses a proto export request from its wire bytes.
    public REQ parseRequest(byte[] bytes) throws InvalidProtocolBufferException {
        return parseRequest.parse(bytes);
    }

    /// Parses a proto export response from its wire bytes.
    public RESP parseResponse(byte[] bytes) throws InvalidProtocolBufferException {
        return parseResponse.parse(bytes);
    }

    /// The four codecs, in wire order (traces, metrics, logs, profiles).
    public static List<SignalCodec<?, ?, ?>> values() {
        return VALUES;
    }

    /// Byte-level proto parse that surfaces the checked protobuf exception. Codec-local so no
    /// transport type leaks in.
    @FunctionalInterface
    public interface Parse<M> {
        M parse(byte[] bytes) throws InvalidProtocolBufferException;
    }
}
