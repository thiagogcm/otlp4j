package dev.nthings.otlp4j.core.signal;

/// Package-private singleton behind [PayloadSizer#reported].
enum ReportedSize implements PayloadSizer {
    INSTANCE;

    @Override
    public long sizeOf(Payload<?> payload) {
        return payload.encodedSize().orElse(0L);
    }
}
