# Bundled OTLP schemas

The `opentelemetry/` subtree is a verbatim mirror of upstream [opentelemetry-proto](https://github.com/open-telemetry/opentelemetry-proto), refreshed by `just update-protos`. Do not hand-edit files inside it; otlp4j notes belong in this file, which sits outside the mirror.

These definitions generate otlp4j's internal message and gRPC service classes: common/resource data and collector services for traces, metrics, logs, and `v1development` profiles.

Generated packages are qualified-exported only to `otlp4j-transport`; they are not part of the application API. See the repository [architecture](../../../../docs/architecture.md) for the boundary and current fidelity limits.
