/// The built-in batch processor and its configuration. The processor buffers incoming payloads,
/// partitioned by the configured metadata keys, and flushes each buffer on an item-count, byte-size,
/// or timeout trigger, merging the buffered domain batches into one downstream payload and handing it
/// to a reusable bounded delivery queue. It honours a send-batch maximum size by splitting an
/// over-large merged batch generically over the signal, and it excludes profiles honestly through the
/// core `SignalSupport` capability, because profiles merge is not lossless across distinct
/// dictionaries.
package dev.nthings.otlp4j.components.batch;
