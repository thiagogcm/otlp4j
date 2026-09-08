package dev.nthings.otlp4j.samples;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("external JPMS consumer")
class ExternalJpmsConsumerTest {

    private static final String MODULE_INFO = """
            module external.consumer {
                requires dev.nthings.otlp4j.core;
                requires dev.nthings.otlp4j.model;
                requires dev.nthings.otlp4j.service;
                requires dev.nthings.otlp4j.transport;
                requires dev.nthings.otlp4j.components;
            }
            """;

    @TempDir
    Path temporaryDirectory;

    @Test
    @DisplayName("compiles a consumer using only the typed pipeline API")
    void compilesTypedConsumer() throws Exception {
        var result = compile("valid", """
                package external.consumer;

                import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

                import dev.nthings.otlp4j.service.Pipeline;
                import dev.nthings.otlp4j.transport.OtlpExporterFactory;
                import java.util.function.UnaryOperator;

                final class Main {
                    static Pipeline<?> create() {
                        return Pipeline.builder(TRACES)
                                .filter(batch -> batch.spanCount() > 0)
                                .map(UnaryOperator.identity())
                                .to(OtlpExporterFactory.grpc())
                                .build();
                    }
                }
                """);

        assertThat(result).isZero();
    }

    @Test
    @DisplayName("compiles a routing processor with its typed configuration")
    void compilesRoutingConfigConsumer() throws Exception {
        var result = compile("routing", """
                package external.consumer;

                import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

                import dev.nthings.otlp4j.components.routing.FilterConfig;
                import dev.nthings.otlp4j.components.routing.FilterProcessorFactory;
                import dev.nthings.otlp4j.components.routing.MatchRule;
                import dev.nthings.otlp4j.components.routing.MatchSpec;
                import dev.nthings.otlp4j.model.TracesData;
                import dev.nthings.otlp4j.service.Pipeline;
                import dev.nthings.otlp4j.transport.OtlpExporterFactory;

                final class Main {
                    static Pipeline<TracesData> create() {
                        var include = MatchSpec.of(MatchRule.builder()
                                .resourceAttribute("service.name", "checkout")
                                .build());
                        return Pipeline.builder(TRACES)
                                .via(new FilterProcessorFactory(), new FilterConfig(include, MatchSpec.none()))
                                .to(OtlpExporterFactory.grpc())
                                .build();
                    }
                }
                """);

        assertThat(result).isZero();
    }

    @Test
    @DisplayName("rejects a component config belonging to another factory")
    void rejectsWrongComponentConfig() throws Exception {
        var result = compile("invalid", """
                package external.consumer;

                import static dev.nthings.otlp4j.core.signal.SignalDescriptor.TRACES;

                import dev.nthings.otlp4j.service.Pipeline;
                import dev.nthings.otlp4j.transport.OtlpExporterFactory;
                import dev.nthings.otlp4j.transport.OtlpReceiverConfig;

                final class Main {
                    static Pipeline<?> create() {
                        return Pipeline.builder(TRACES)
                                .to(OtlpExporterFactory.grpc(), OtlpReceiverConfig.defaults(4317))
                                .build();
                    }
                }
                """);

        assertThat(result).isNotZero();
    }

    private int compile(String name, String mainSource) throws Exception {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertThat(compiler).as("a JDK compiler is required for this JPMS consumer test").isNotNull();

        var sourceDirectory = temporaryDirectory.resolve(name).resolve("source");
        var outputDirectory = temporaryDirectory.resolve(name).resolve("classes");
        Files.createDirectories(sourceDirectory.resolve("external/consumer"));
        Files.createDirectories(outputDirectory);
        Files.writeString(sourceDirectory.resolve("module-info.java"), MODULE_INFO);
        Files.writeString(sourceDirectory.resolve("external/consumer/Main.java"), mainSource);

        var moduleLocations = ModuleLayer.boot().configuration().modules().stream()
                .map(resolved -> resolved.reference().location())
                .flatMap(Optional::stream)
                .filter(location -> "file".equalsIgnoreCase(location.getScheme()))
                .map(Path::of);
        var classpathLocations = Arrays.stream(System.getProperty("java.class.path").split(File.pathSeparator))
                .map(Path::of);
        var modulePath = Stream.concat(moduleLocations, classpathLocations)
                .filter(Files::exists)
                .distinct()
                .map(Path::toString)
                .collect(Collectors.joining(File.pathSeparator));
        return compiler.run(
                null,
                null,
                null,
                "--release",
                "25",
                "--module-path",
                modulePath,
                "-d",
                outputDirectory.toString(),
                sourceDirectory.resolve("module-info.java").toString(),
                sourceDirectory.resolve("external/consumer/Main.java").toString());
    }
}
