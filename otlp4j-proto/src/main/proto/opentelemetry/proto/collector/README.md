# Bundled OTLP schemas

This tree contains the OpenTelemetry Protobuf definitions used to generate otlp4j's internal message and gRPC service classes. It includes common/resource data and collector services for traces, metrics, logs, and `v1development` profiles.

Generated packages are qualified-exported only to `otlp4j-transport`; they are not part of the application API. See the repository [architecture](../../../../../../../docs/architecture.md) for the boundary and current fidelity limits.
