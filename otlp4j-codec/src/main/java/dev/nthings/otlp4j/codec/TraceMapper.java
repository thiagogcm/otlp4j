package dev.nthings.otlp4j.codec;

import dev.nthings.otlp4j.model.Span;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.model.ConsumeResult;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span.Event;
import io.opentelemetry.proto.trace.v1.Span.Link;
import io.opentelemetry.proto.trace.v1.Span.SpanKind;
import java.util.ArrayList;

/// Maps OTLP trace types between proto and domain, in both directions.
///
/// Internal. Part of the transport layer; not public API.
public final class TraceMapper {

    private TraceMapper() {}

    // --- proto -> domain ---

    public static TracesData toDomain(ExportTraceServiceRequest request) {
        var resourceSpans = new ArrayList<TracesData.ResourceSpans>(request.getResourceSpansCount());
        for (var rs : request.getResourceSpansList()) {
            resourceSpans.add(toDomain(rs));
        }
        return new TracesData(resourceSpans);
    }

    private static TracesData.ResourceSpans toDomain(
            ResourceSpans rs) {
        var scopeSpans = new ArrayList<TracesData.ScopeSpans>(rs.getScopeSpansCount());
        for (var ss : rs.getScopeSpansList()) {
            scopeSpans.add(toDomain(ss));
        }
        return new TracesData.ResourceSpans(
                CommonMapper.resource(rs.getResource()), rs.getSchemaUrl(), scopeSpans);
    }

    private static TracesData.ScopeSpans toDomain(ScopeSpans ss) {
        var spans = new ArrayList<Span>(ss.getSpansCount());
        for (var span : ss.getSpansList()) {
            spans.add(toDomain(span));
        }
        return new TracesData.ScopeSpans(
                CommonMapper.scope(ss.getScope()), ss.getSchemaUrl(), spans);
    }

    private static Span toDomain(io.opentelemetry.proto.trace.v1.Span span) {
        var events = new ArrayList<Span.Event>(span.getEventsCount());
        for (var event : span.getEventsList()) {
            events.add(new Span.Event(
                    event.getTimeUnixNano(),
                    event.getName(),
                    CommonMapper.attributes(event.getAttributesList()),
                    event.getDroppedAttributesCount()));
        }
        var links = new ArrayList<Span.Link>(span.getLinksCount());
        for (var link : span.getLinksList()) {
            links.add(new Span.Link(
                    CommonMapper.hex(link.getTraceId()),
                    CommonMapper.hex(link.getSpanId()),
                    link.getTraceState(),
                    CommonMapper.attributes(link.getAttributesList()),
                    link.getDroppedAttributesCount(),
                    Integer.toUnsignedLong(link.getFlags())));
        }
        return new Span(
                CommonMapper.hex(span.getTraceId()),
                CommonMapper.hex(span.getSpanId()),
                CommonMapper.hex(span.getParentSpanId()),
                span.getTraceState(),
                Integer.toUnsignedLong(span.getFlags()),
                span.getName(),
                Span.Kind.fromNumber(span.getKindValue()),
                span.getStartTimeUnixNano(),
                span.getEndTimeUnixNano(),
                CommonMapper.attributes(span.getAttributesList()),
                span.getDroppedAttributesCount(),
                events,
                span.getDroppedEventsCount(),
                links,
                span.getDroppedLinksCount(),
                status(span.getStatus()));
    }

    private static Span.Status status(io.opentelemetry.proto.trace.v1.Status status) {
        // Raw-int accessor: UNRECOGNIZED degrades to UNSET instead of throwing.
        return new Span.Status(
                Span.Status.Code.fromNumber(status.getCodeValue()), status.getMessage());
    }

    /// Interprets an OTLP trace export response as a [ConsumeResult].
    public static ConsumeResult result(ExportTraceServiceResponse response) {
        var partial = response.getPartialSuccess();
        return CommonMapper.result(
                response.hasPartialSuccess(), partial.getRejectedSpans(), partial.getErrorMessage());
    }

    // --- domain -> proto ---

    public static ExportTraceServiceRequest toProto(TracesData traces) {
        var request = ExportTraceServiceRequest.newBuilder();
        for (var rs : traces.resourceSpans()) {
            request.addResourceSpans(toProto(rs));
        }
        return request.build();
    }

    private static ResourceSpans toProto(
            TracesData.ResourceSpans rs) {
        var builder =
                ResourceSpans.newBuilder()
                        .setResource(CommonMapper.toProtoResource(rs.resource()))
                        .setSchemaUrl(rs.schemaUrl());
        for (var ss : rs.scopeSpans()) {
            builder.addScopeSpans(toProto(ss));
        }
        return builder.build();
    }

    private static ScopeSpans toProto(TracesData.ScopeSpans ss) {
        var builder =
                ScopeSpans.newBuilder()
                        .setScope(CommonMapper.toProtoScope(ss.scope()))
                        .setSchemaUrl(ss.schemaUrl());
        for (var span : ss.spans()) {
            builder.addSpans(toProto(span));
        }
        return builder.build();
    }

    private static io.opentelemetry.proto.trace.v1.Span toProto(Span span) {
        var builder =
                io.opentelemetry.proto.trace.v1.Span.newBuilder()
                        .setTraceId(CommonMapper.bytes(span.traceId()))
                        .setSpanId(CommonMapper.bytes(span.spanId()))
                        .setParentSpanId(CommonMapper.bytes(span.parentSpanId()))
                        .setTraceState(span.traceState())
                        .setFlags((int) span.flags())
                        .setName(span.name())
                        .setKind(SpanKind.forNumber(
                                span.kind().number()))
                        .setStartTimeUnixNano(span.startEpochNanos())
                        .setEndTimeUnixNano(span.endEpochNanos())
                        .addAllAttributes(CommonMapper.toKeyValues(span.attributes()))
                        .setDroppedAttributesCount(span.droppedAttributesCount())
                        .setDroppedEventsCount(span.droppedEventsCount())
                        .setDroppedLinksCount(span.droppedLinksCount())
                        .setStatus(io.opentelemetry.proto.trace.v1.Status.newBuilder()
                                .setCode(io.opentelemetry.proto.trace.v1.Status.StatusCode
                                        .forNumber(span.status().code().number()))
                                .setMessage(span.status().message()));
        for (var event : span.events()) {
            builder.addEvents(Event.newBuilder()
                    .setTimeUnixNano(event.epochNanos())
                    .setName(event.name())
                    .addAllAttributes(CommonMapper.toKeyValues(event.attributes()))
                    .setDroppedAttributesCount(event.droppedAttributesCount()));
        }
        for (var link : span.links()) {
            builder.addLinks(Link.newBuilder()
                    .setTraceId(CommonMapper.bytes(link.traceId()))
                    .setSpanId(CommonMapper.bytes(link.spanId()))
                    .setTraceState(link.traceState())
                    .addAllAttributes(CommonMapper.toKeyValues(link.attributes()))
                    .setDroppedAttributesCount(link.droppedAttributesCount())
                    .setFlags((int) link.flags()));
        }
        return builder.build();
    }
}
