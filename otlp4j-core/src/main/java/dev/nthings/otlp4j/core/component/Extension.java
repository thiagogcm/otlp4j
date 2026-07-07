package dev.nthings.otlp4j.core.component;

/// An extension: a component that provides a cross-cutting capability with no telemetry data path of
/// its own, for example an authenticator. It is a `Component` with the deadline-bounded lifecycle
/// and is started before, and shut down after, the data-path components. This is the contract only
/// in the first milestone; the first concrete extensions land in a later phase, but the kind and
/// lifecycle slot exist now.
public interface Extension extends Component {
}
