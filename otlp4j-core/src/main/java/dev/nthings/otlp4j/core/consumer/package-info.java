/// The signal consumer contract and the per-invocation delivery context. `SignalConsumer` is the
/// single typed consume interface that replaces the legacy `Sink` and its four per-signal variants;
/// `ConsumerCapabilities` declares whether a consumer mutates its input so the fan-out node can copy
/// ahead of it. `ConsumeContext` carries the receiver-captured client metadata, the deadline, and
/// the cancellation signal that appear in every consume call; `ClientMetadata` and `Deadline` are
/// its parts. `SignalConsumers` is the multi-signal bundle a receiver factory receives so one
/// receiver instance serves every routed signal from one port.
@NullMarked
package dev.nthings.otlp4j.core.consumer;

import org.jspecify.annotations.NullMarked;
