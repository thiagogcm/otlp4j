# Development

Run commands in this guide from the repository root. Use JDK 25 and the Maven wrapper. The wrapper selects Maven 3.9.16; the build accepts Maven 3.9.9 or newer. `just` is optional and wraps the Maven commands in [justfile](../justfile).

## Build and check

| Task                                           | Command             |
| ---------------------------------------------- | ------------------- |
| Check the Java and Maven toolchain             | `./mvnw --version`  |
| Compile the default reactor                    | `./mvnw -B compile` |
| Run its tests                                  | `./mvnw -B test`    |
| Run tests, coverage checks, and Javadoc lint   | `./mvnw -B verify`  |
| Install artifacts locally                      | `./mvnw -B install` |
| Check justfile formatting and run verification | `just ci`           |

For one test class and its module dependencies:

```sh
./mvnw -B -pl otlp4j-samples -am test \
  -Dtest=OtlpE2eDemoTest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

Replace the module and test name as needed. See [Sample scenarios](../otlp4j-samples/README.md) for what this test demonstrates.

## Test the boundary affected by a change

Read related tests before adding cases. Reuse setup and focus assertions on failures that would expose an implementation defect, rather than repeating validations already covered elsewhere.

| Change | Relevant checks |
| --- | --- |
| Domain values or codec mappings | Constructor invariants, round trips, property tests, item-count and routing parity |
| Graph assembly | Diagnostics, signal support, sharing, connector pairs, and cycle rejection |
| Buffering or concurrency | Admission, overflow, loss reports, flush barriers, late outcomes, and worker failure |
| Wire transport | HTTP/gRPC conformance, compression, size limits, TLS, retries, and server hints |
| Lifecycle | Startup rollback, shutdown ordering, deadlines, and resource cleanup |
| SDK adapters | Snapshot isolation, field mappings, result translation, and borrowed-input lifecycle |
| Application API | Sample tests and the external JPMS consumer test |

The `otlp4j-testing` module supplies fixtures and reusable conformance suites. `otlp4j-conformance` runs them against the bundled transport through real wire clients. Test TLS fixture maintenance is documented [beside the PEM resources](../otlp4j-testing/src/main/resources/dev/nthings/otlp4j/testing/conformance/tls/README.md).

Verification enforces module coverage thresholds where configured, currently 90% instruction coverage and 80% branch coverage. The aggregate report is written to `otlp4j-coverage/target/site/jacoco-aggregate/index.html`. Coverage supports inspection; meaningful failure tests remain necessary.

## Generate protocol sources

The proto module's `src/main/proto/opentelemetry/` subtree mirrors the checked-out upstream schema source. Do not hand-edit the mirror. Refresh it through:

```sh
just update-protos
just proto
```

The first recipe initializes the schema submodule and copies its `opentelemetry/` directory. It copies the selected checkout; it does not choose a newer upstream revision. The second recipe generates protobuf and gRPC sources.

Without `just`, source generation is:

```sh
./mvnw -B -pl otlp4j-proto -am generate-sources
```

Retain upstream license headers and keep project-specific notes [outside the mirror](../otlp4j-proto/src/main/proto/README.md). After a schema change, verify codec mappings and wire behavior for every affected signal.

## Optional builds

The default reactor excludes JMH benchmarks. Build and run them through `just bench`, or use the commands and measurement guidance in [Performance measurements](../otlp4j-benchmarks/baselines/README.md). Performance claims need measurements for the relevant workload and environment.

To package the end-to-end sample as a GraalVM native image:

```sh
./mvnw -B -pl otlp4j-samples -am package -Pnative
```

This profile requires a GraalVM JDK and selects `OtlpE2eDemo` as the entry point. It is separate from JPMS runtime-image packaging.

## Maintain the guides

Organize documentation around readers' decisions and tasks, following the reader focus of [ISO 24495-1's plain-language principles](https://www.iplfederation.org/iso-standard/). Keep current behavior and practical limits easy to locate and apply.

- Put each rule or default in one guide and link to it from other pages.
- Use focused paragraphs for explanations, tables for exact mappings, and lists for actions or choices.
- Use diagrams for relationships and event order; choose the Mermaid type that fits the information.
- Describe supported behavior and requirements without change journals or em dashes.
- Check example signatures against source and validate links after reorganizing sections.
- Run Prettier on edited Markdown files only, using the installed configuration.

For example, after editing one guide:

```sh
prettier --write docs/pipelines.md
prettier --check docs/pipelines.md
```
