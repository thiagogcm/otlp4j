# Bundled OTLP schemas

The `opentelemetry/` subtree mirrors the checked-out upstream [opentelemetry-proto](https://github.com/open-telemetry/opentelemetry-proto) schema source. It includes common and resource data, traces, metrics, logs, experimental `v1development` profiles, and collector-service definitions.

Do not hand-edit files inside the mirror. Keep project-specific notes in this file and use the [schema-generation workflow](../../../../docs/development.md#generate-protocol-sources) to refresh and generate sources. Preserve the mirrored files' upstream Apache License 2.0 headers.

Generated classes are internal implementation dependencies. JPMS grants access to codec and transport, with service packages also available to Armeria's gRPC module. Applications use the public [domain model](../../../../docs/model.md) and [payload contracts](../../../../docs/public-api.md#core-contracts). See [Architecture](../../../../docs/architecture.md#jpms-encapsulation) for the boundary.
