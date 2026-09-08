# Bundled OTLP schemas

The `opentelemetry/` subtree is a verbatim mirror of upstream [opentelemetry-proto v1.11.0](https://github.com/open-telemetry/opentelemetry-proto/tree/v1.11.0), refreshed by `just update-protos`. Do not hand-edit files inside it; otlp4j notes belong in this file, which sits outside the mirror. The mirrored schemas remain available under the upstream [Apache License 2.0](https://github.com/open-telemetry/opentelemetry-proto/blob/v1.11.0/LICENSE).

These definitions generate otlp4j's internal message and gRPC service classes: common/resource data and collector services for traces, metrics, logs, and `v1development` profiles.

Generated packages are qualified-exported only to the codec and transport modules (`otlp4j-codec`, `otlp4j-transport`); they are not part of the application API. See the repository [architecture](../../../../docs/architecture.md) for the boundary and current fidelity limits.
