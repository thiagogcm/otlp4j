package dev.nthings.otlp4j.components.support;

import dev.nthings.otlp4j.core.signal.PayloadView;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import dev.nthings.otlp4j.core.signal.TelemetryPayload;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/// A [TelemetryPayload] decorator that records whether [PayloadView#DOMAIN_IMMUTABLE] was ever
/// requested through it, regardless of the delegate's own materialisation caching or backing. This lets
/// the routing and filtering tests prove, at the match-engine and processor level, that a
/// resource-attribute or header rule never calls for the domain view that only the domain-predicate
/// escape hatch is allowed to force; the property holds independently of whether the wrapped delegate
/// is proto-backed or domain-backed, since it is a statement about which view the matching code reads,
/// not about how expensive that read happens to be for a given delegate.
public final class MaterialisationTrackingPayload implements TelemetryPayload {

    private final TelemetryPayload delegate;
    private final AtomicBoolean domainRequested = new AtomicBoolean();

    /// Wraps `delegate`, tracking its [PayloadView#DOMAIN_IMMUTABLE] reads.
    ///
    /// @param delegate the payload to wrap
    public MaterialisationTrackingPayload(TelemetryPayload delegate) {
        this.delegate = delegate;
    }

    /// Whether [PayloadView#DOMAIN_IMMUTABLE] has been requested through this wrapper at least once.
    ///
    /// @return true once the domain view has been read
    public boolean domainRequested() {
        return domainRequested.get();
    }

    @Override
    public SignalDescriptor<?> signal() {
        return delegate.signal();
    }

    @Override
    public long itemCount() {
        return delegate.itemCount();
    }

    @Override
    public long estimatedSerializedSize() {
        return delegate.estimatedSerializedSize();
    }

    @Override
    public <V> V view(PayloadView<V> view) {
        if (view == PayloadView.DOMAIN_IMMUTABLE) {
            domainRequested.set(true);
        }
        return delegate.view(view);
    }

    @Override
    public TelemetryPayload copyForMutation() {
        return new MaterialisationTrackingPayload(delegate.copyForMutation());
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        delegate.writeTo(out);
    }
}
