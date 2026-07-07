package dev.nthings.otlp4j.service;

import dev.nthings.otlp4j.core.component.ComponentId;
import dev.nthings.otlp4j.core.component.ComponentKind;

/// The composite identity the runtime keys its live-component lookup by: a [ComponentKind] together
/// with a [ComponentId]. Two components of different kinds may share the same id in a Collector-style
/// configuration (an `otlp` receiver and an `otlp` exporter, for example), so the id alone is not a
/// unique key across the whole gateway; the kind disambiguates them. Package-private runtime detail.
///
/// @param kind the component kind
/// @param id   the component id
record ComponentKey(ComponentKind kind, ComponentId id) {}
