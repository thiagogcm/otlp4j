package dev.nthings.otlp4j.components.support;

import dev.nthings.otlp4j.core.signal.Payload;
import dev.nthings.otlp4j.core.signal.RoutingMetadata;
import dev.nthings.otlp4j.core.signal.SignalDescriptor;
import java.util.concurrent.atomic.AtomicBoolean;

/// A payload decorator that records whether a routing rule requested the materialised domain.
public final class MaterialisationTrackingPayload<T> implements Payload<T> {

    private final Payload<T> delegate;
    private final AtomicBoolean dataRequested = new AtomicBoolean();

    public MaterialisationTrackingPayload(Payload<T> delegate) {
        this.delegate = delegate;
    }

    public boolean domainRequested() {
        return dataRequested.get();
    }

    @Override
    public SignalDescriptor<T> signal() {
        return delegate.signal();
    }

    @Override
    public T data() {
        dataRequested.set(true);
        return delegate.data();
    }

    @Override
    public long itemCount() {
        return delegate.itemCount();
    }

    @Override
    public RoutingMetadata routingMetadata() {
        return delegate.routingMetadata();
    }

    @Override
    public Payload<T> withData(T replacement) {
        return new MaterialisationTrackingPayload<>(delegate.withData(replacement));
    }
}
