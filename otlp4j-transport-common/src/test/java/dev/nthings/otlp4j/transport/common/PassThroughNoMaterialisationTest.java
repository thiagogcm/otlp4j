package dev.nthings.otlp4j.transport.common;

import static org.assertj.core.api.Assertions.assertThat;

import dev.nthings.otlp4j.core.result.DeliveryResult;
import dev.nthings.otlp4j.core.result.Provenance;
import dev.nthings.otlp4j.core.signal.PayloadMemoryMode;
import dev.nthings.otlp4j.core.signal.PayloadView;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import dev.nthings.otlp4j.model.TracesData;
import dev.nthings.otlp4j.spi.OtlpClient;
import java.io.OutputStream;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// End-to-end proof of the Phase 2 exit criterion at the exporter boundary: a same-protocol
/// no-mutation pass-through export forwards a proto-backed payload from the exporter facet to the
/// client without ever asking for its domain view, so a `TracesData` is never materialised unless a
/// processor requests it.
///
/// The seam is an instrumented [TelemetryPayload] that records whether [PayloadView#DOMAIN_IMMUTABLE]
/// is ever requested. It is driven through a real [ClientExporter] facet into a client that reads only
/// [PayloadView#PROTO_REQUEST], mirroring how the gRPC and HTTP clients read a pass-through payload.
@DisplayName("pass-through export does not materialise the domain")
class PassThroughNoMaterialisationTest {

    @DisplayName("a receiver-to-exporter pass-through never requests the domain view")
    @Test
    void passThroughExportNeverMaterialisesTheDomain() {
        var payload = new InstrumentedProtoPayload();

        try (var exporter = new ClientExporter(new ProtoReadingClient(), "test-passthrough")) {
            var result = exporter.consumer(SignalDescriptor.TRACES)
                    .consume(new TestConsumeContext(), payload)
                    .toCompletableFuture().join();
            assertThat(result).isInstanceOf(DeliveryResult.Accepted.class);
        }

        assertThat(payload.domainViewed()).isFalse();
    }

    @DisplayName("the seam flips once the domain view is genuinely requested")
    @Test
    void seamFlipsWhenDomainIsViewed() {
        var payload = new InstrumentedProtoPayload();

        assertThat(payload.domainViewed()).isFalse();
        payload.view(PayloadView.DOMAIN_IMMUTABLE);
        assertThat(payload.domainViewed()).isTrue();
    }

    /// A client that reads only the proto request view, the way a same-protocol pass-through export
    /// writes the carried message straight to the wire.
    private static final class ProtoReadingClient implements OtlpClient {

        @Override
        public CompletionStage<DeliveryResult> export(TelemetryPayload payload, Provenance provenance) {
            payload.view(PayloadView.PROTO_REQUEST);
            return DeliveryResult.acceptedStage(provenance);
        }

        @Override
        public CompletionStage<Void> shutdown(Duration timeout) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /// A proto-backed payload seam: it answers the pass-through operations cheaply and flags any
    /// request for the domain view, so the test can assert the export path never triggered one.
    private static final class InstrumentedProtoPayload implements TelemetryPayload {

        private final AtomicBoolean domainViewed = new AtomicBoolean();
        private final Object protoRequest = new Object();

        @Override
        public SignalDescriptor<?> signal() {
            return SignalDescriptor.TRACES;
        }

        @Override
        public long itemCount() {
            return 0L;
        }

        @Override
        public long estimatedSerializedSize() {
            return 0L;
        }

        @Override
        public PayloadMemoryMode memoryMode() {
            return PayloadMemoryMode.PROTO_BACKED;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <V> V view(PayloadView<V> view) {
            if (view == PayloadView.PROTO_REQUEST) {
                return (V) protoRequest;
            }
            if (view == PayloadView.DOMAIN_IMMUTABLE) {
                domainViewed.set(true);
                return (V) new TracesData(List.of());
            }
            throw new IllegalArgumentException("unsupported payload view: " + view);
        }

        @Override
        public TelemetryPayload copyForMutation() {
            return this;
        }

        @Override
        public void writeTo(OutputStream out) {
            // No bytes: the test never exercises the wire, only which view the export path reads.
        }

        boolean domainViewed() {
            return domainViewed.get();
        }
    }
}
