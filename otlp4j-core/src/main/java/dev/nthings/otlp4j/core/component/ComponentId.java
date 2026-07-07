package dev.nthings.otlp4j.core.component;

/// A component's identity: a `type` and an optional `name`, in the OpenTelemetry Collector
/// `type[/name]` string form. `otlp` names the default instance of the `otlp` component;
/// `otlp/secondary` names a second, distinctly configured instance of the same component type. The
/// value is used as a graph node identity and as a `Map` key, so it is a plain value record with the
/// canonical equality a record gives.
///
/// The `name` is the empty string when a component is referenced by bare type. Parsing splits on the
/// first `/`, so a name may itself contain slashes; rendering with [#toString] reproduces the
/// original string form.
///
/// @param type the component type, never blank
/// @param name the instance name, or the empty string for the default instance
public record ComponentId(String type, String name) {

    /// Validates the components: `type` must be present and non-blank, and `name` is normalized to
    /// the empty string when null.
    public ComponentId {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("component type must be non-blank");
        }
        name = name == null ? "" : name;
    }

    /// Parses the Collector `type[/name]` string form. `"otlp"` yields type `otlp` with an empty
    /// name; `"otlp/secondary"` yields type `otlp` and name `secondary`. The split is on the first
    /// `/` only.
    ///
    /// @param id the `type[/name]` string
    /// @return the parsed identity
    public static ComponentId parse(String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("component id must be non-blank");
        }
        var slash = id.indexOf('/');
        if (slash < 0) {
            return new ComponentId(id, "");
        }
        return new ComponentId(id.substring(0, slash), id.substring(slash + 1));
    }

    /// Renders back the `type[/name]` string form: `type` alone when the name is empty, otherwise
    /// `type/name`.
    @Override
    public String toString() {
        return name.isEmpty() ? type : type + "/" + name;
    }
}
