set default-list
set dotenv-load
set shell := ["bash", "-euo", "pipefail", "-c"]

mvn := "./mvnw"
batch := "-B"
modules := "otlp4j-model otlp4j-proto otlp4j-core otlp4j-codec otlp4j-transport-common otlp4j-service otlp4j-transport-otlp otlp4j-samples otlp4j-testing otlp4j-conformance otlp4j-coverage otlp4j-benchmarks"
sample_module := "otlp4j-samples"
sample_test := "OtlpE2eDemoTest"
coverage_index := "otlp4j-coverage/target/site/jacoco-aggregate/index.html"

alias b := build
alias c := ci
alias t := test
alias v := verify

# List recipes in source order.
help:
    @just --list --unsorted

# Format this justfile.
fmt:
    just --fmt

# Check justfile formatting without changing files.
fmt-check:
    just --fmt --check

# Print the local Java, Maven, and just toolchain versions.
doctor:
    @{{ mvn }} --version
    @just --version

# Run Maven in batch mode with arbitrary arguments.
maven *args:
    {{ mvn }} {{ batch }} {{ args }}

# Print reactor module names.
modules:
    @printf '%s\n' {{ modules }}

# Remove Maven build output.
clean:
    {{ mvn }} {{ batch }} clean

# Compile all modules.
compile:
    {{ mvn }} {{ batch }} compile

# Build packages without running tests.
build:
    {{ mvn }} {{ batch }} package -DskipTests

# Run all tests.
test:
    {{ mvn }} {{ batch }} test

# Run tests for one module and anything it needs.
test-module module:
    {{ mvn }} {{ batch }} -pl {{ module }} -am test

# Run one test class in one module.
test-class module test:
    {{ mvn }} {{ batch }} -pl {{ module }} -am test -Dtest={{ test }} -Dsurefire.failIfNoSpecifiedTests=false

# Run the end-to-end sample canary.
sample-test:
    {{ mvn }} {{ batch }} -pl {{ sample_module }} -am test -Dtest={{ sample_test }} -Dsurefire.failIfNoSpecifiedTests=false

# Run the full reactor verification, including coverage gates and Javadoc lint.
verify:
    {{ mvn }} {{ batch }} verify

# CI entrypoint: justfile formatting plus the full Maven verification.
ci: fmt-check verify

# Generate the aggregate coverage report and print its local path.
coverage:
    {{ mvn }} {{ batch }} verify
    @printf 'Coverage report: %s\n' '{{ coverage_index }}'

# Build the GraalVM native-image sample profile.
native:
    {{ mvn }} {{ batch }} -pl {{ sample_module }} -am package -Pnative

# Build the jlink sample runtime image profile.
jlink:
    {{ mvn }} {{ batch }} -pl {{ sample_module }} -am package -Pjlink

# Regenerate the conformance TLS material by running the committed openssl script directly, so its
# `#!/usr/bin/env bash` shebang is honored rather than the recipe interpreter. Only needed if the
# test CA is rotated.
conformance-certs:
    ./otlp4j-testing/src/main/resources/dev/nthings/otlp4j/testing/conformance/tls/regen.sh

# Build the JMH uber jar and run all benchmarks with the GC profiler. The `bench` profile keeps the
# benchmark module out of the default reactor; run/warmup/measurement counts are pinned on the
# benchmark classes, so a plain run stays short (see otlp4j-benchmarks/baselines/README.md).
bench:
    {{ mvn }} {{ batch }} -Pbench -pl otlp4j-benchmarks -am package -DskipTests
    java -jar otlp4j-benchmarks/target/benchmarks.jar -prof gc

# Record a dated baseline as JSON plus the GC profiler, committed under baselines/. Produce these
# numbers on a quiet machine, never on shared CI.
bench-baseline date=`date +%Y-%m-%d`:
    {{ mvn }} {{ batch }} -Pbench -pl otlp4j-benchmarks -am package -DskipTests
    java -jar otlp4j-benchmarks/target/benchmarks.jar -prof gc \
        -rf json -rff otlp4j-benchmarks/baselines/baseline-{{ date }}.json

# Update the opentelemetry-proto submodule and copy protos into the proto module.
# The copied opentelemetry/ subtree is a verbatim mirror: don't hand-edit it.
# otlp4j notes live in otlp4j-proto/src/main/proto/README.md, outside the mirror.
update-protos:
    git submodule update --remote --force --init -- opentelemetry-proto
    cp -r opentelemetry-proto/opentelemetry otlp4j-proto/src/main/proto/

# Generate protobuf and gRPC sources.
proto:
    {{ mvn }} {{ batch }} -pl otlp4j-proto -am generate-sources

# Install the reactor artifacts into the local Maven repository.
install:
    {{ mvn }} {{ batch }} install

# Print a dependency tree for the whole reactor or one module.
deps module='':
    @if [ -n '{{ module }}' ]; then \
        {{ mvn }} {{ batch }} -pl '{{ module }}' -am dependency:tree; \
    else \
        {{ mvn }} {{ batch }} dependency:tree; \
    fi
