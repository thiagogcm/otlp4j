/// The declarative gateway configuration model: the intermediate form every construction path
/// compiles to.
///
/// [dev.nthings.otlp4j.service.config.GatewayConfig] holds the component instances available by id,
/// grouped into five kind maps, the pipelines that wire them, and the
/// [dev.nthings.otlp4j.service.config.ServiceOptions] that bracket the lifecycle. Each component is a
/// [dev.nthings.otlp4j.service.config.ReceiverEntry] and its four siblings, a factory paired with its
/// typed config so the config is checked against its factory at compile time. A
/// [dev.nthings.otlp4j.service.config.ComponentRegistry] over a config answers the existence and
/// signal-support queries validation asks, consulting the optional
/// [dev.nthings.otlp4j.core.component.SignalSupport] capability a factory may declare.
@NullMarked
package dev.nthings.otlp4j.service.config;

import org.jspecify.annotations.NullMarked;
