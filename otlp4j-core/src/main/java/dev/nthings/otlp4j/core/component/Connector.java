package dev.nthings.otlp4j.core.component;

/// A connector: a component that bridges two pipelines by consuming one signal and emitting another,
/// for example deriving a span-count metric from a traces stream. It is a `Component` with the
/// deadline-bounded lifecycle; the signal pairing it bridges is declared by its factory and carried
/// by the graph as edge metadata.
public interface Connector extends Component {
}
