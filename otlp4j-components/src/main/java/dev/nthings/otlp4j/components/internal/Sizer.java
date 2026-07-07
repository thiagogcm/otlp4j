package dev.nthings.otlp4j.components.internal;

/// Which running total a batch flush trigger reads: the accumulated OTLP item count or the
/// accumulated serialized byte size. A batch accumulator tracks both totals as payloads arrive, and
/// each size-based flush trigger selects the total it compares against its threshold through a sizer,
/// so the trigger logic names neither total directly.
///
/// This lives internal to the module because the payload already exposes both counts and no core
/// `Sizer` type is needed; it is a small selector over two `long` totals rather than a public policy.
public enum Sizer {

    /// Reads the accumulated OTLP item count (spans, data points, or log records).
    ITEMS {
        @Override
        public long read(long items, long bytes) {
            return items;
        }
    },

    /// Reads the accumulated serialized byte size.
    BYTES {
        @Override
        public long read(long items, long bytes) {
            return bytes;
        }
    };

    /// Selects this sizer's running total from the two totals a batch accumulator tracks.
    ///
    /// @param items the accumulated item count
    /// @param bytes the accumulated serialized byte size
    /// @return the total this sizer reads
    public abstract long read(long items, long bytes);
}
