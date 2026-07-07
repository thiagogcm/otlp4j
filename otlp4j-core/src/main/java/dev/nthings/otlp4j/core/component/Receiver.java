package dev.nthings.otlp4j.core.component;

/// A receiver: a component that ingests telemetry from a transport into the graph. It is a
/// `Component`, so it has the deadline-bounded start, flush, and shutdown lifecycle. A receiver that
/// binds a network socket also implements [NetworkBound] so a caller can read its bound port after
/// start.
public interface Receiver extends Component {
}
