/// The declarative configuration binding SPI, dependency-free over the JDK so no wire or parser
/// library appears on this surface. `ConfigNode` is an immutable, key-tracking tree over one parsed
/// configuration sub-map, with typed accessors and an unread-key report that makes unknown-key
/// diagnostics free. `ConfigBinder` is the optional capability a component factory implements to fold
/// its own sub-map onto its typed config record, type-safely and without reflection. A declarative
/// loader in a separate module builds the nodes from its parser output and drives the binders; this
/// package names neither the loader nor the parser.
@NullMarked
package dev.nthings.otlp4j.core.config;

import org.jspecify.annotations.NullMarked;
