package dev.nthings.otlp4j.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import dev.nthings.otlp4j.core.policy.RetryPolicy;
import dev.nthings.otlp4j.core.policy.TimeoutPolicy;
import dev.nthings.otlp4j.transport.OtlpExporter;
import dev.nthings.otlp4j.transport.OtlpExporterConfig;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.module.ModuleDescriptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Locks the negative space of the exporter runtime: the public surface never names a Resilience4j
/// type, no sending queue, circuit breaker, or bulkhead type exists anywhere in the exporter modules,
/// the exporter config surface exposes only the neutral retry and timeout policies, and neither
/// transport-common nor the consolidated transport carries a transitive Resilience4j module
/// requirement.
/// Every check reads the compiled classes and module descriptors directly rather than trusting a hand
/// written inventory, so a future change that reintroduces any of these shapes fails this test instead
/// of drifting past review unnoticed.
///
/// This module is deliberately non modular (see its Maven description), so a class loaded here always
/// reports back an unnamed module and [Class#getModule()] cannot answer the transitive requirement
/// question. The transitive requirement guard therefore resolves each representative class's code
/// source location and reads the real compiled `module-info.class` found there directly with
/// [ModuleDescriptor#read(InputStream)], which works identically whether the location is a
/// `target/classes` directory in the reactor build or a packaged jar in a resolved build.
final class ExporterRuntimeNegativeSpaceTest {

    private static final String RESILIENCE4J_PREFIX = "io.github.resilience4j";

    /// The single package the consolidated transport exports. This is the whole public exporter
    /// surface a caller can see.
    private static final List<String> EXPORTED_EXPORTER_PACKAGES = List.of(
            "dev.nthings.otlp4j.transport");

    /// Concept names that must never appear as a type's simple name anywhere in the exporter runtime,
    /// public or internal: a sending queue, a circuit breaker, and a bulkhead are the negative space
    /// the batch delivery queue and its degraded-window refusal are meant to be the only stand-ins for.
    private static final List<String> BANNED_SIMPLE_NAME_FRAGMENTS =
            List.of("SendingQueue", "CircuitBreaker", "Bulkhead");

    /// A representative exported class of the consolidated transport module, used both to resolve the
    /// module's compiled classes root for the whole-module class scan and to locate its
    /// `module-info.class` for the transitive requirement guard.
    private static final List<Class<?>> EXPORTER_RUNTIME_MODULE_REPRESENTATIVES =
            List.of(OtlpExporter.class);

    @DisplayName("no public exporter type's signature names a Resilience4j type")
    @Test
    void publicExporterSurfaceNamesNoResilience4jType() {
        List<String> offenders = new ArrayList<>();
        for (String packageName : EXPORTED_EXPORTER_PACKAGES) {
            for (Class<?> type : classesInPackage(packageName)) {
                if (Modifier.isPublic(type.getModifiers())) {
                    collectResilience4jSignatures(type, offenders);
                }
            }
        }
        assertThat(offenders)
                .as("public exporter types whose constructor, method, or field names a Resilience4j type")
                .isEmpty();
    }

    @DisplayName("no sending queue, circuit breaker, or bulkhead type exists in the exporter runtime")
    @Test
    void exporterModulesContainNoSendingQueueCircuitBreakerOrBulkheadType() {
        List<String> offenders = new ArrayList<>();
        for (Class<?> representative : EXPORTER_RUNTIME_MODULE_REPRESENTATIVES) {
            File classesRoot = codeSourceLocation(representative);
            for (String classFileName : allClassFileNames(classesRoot)) {
                String simpleName = simpleNameOf(classFileName);
                for (String banned : BANNED_SIMPLE_NAME_FRAGMENTS) {
                    if (simpleName.contains(banned)) {
                        offenders.add(representative.getName() + "'s module carries " + classFileName
                                + ", whose simple name contains the banned concept " + banned);
                    }
                }
            }
        }
        assertThat(offenders)
                .as("sending queue, circuit breaker, or bulkhead types found in the exporter runtime")
                .isEmpty();
    }

    @DisplayName("the exporter config surface exposes only the neutral retry and timeout policies")
    @Test
    void exporterConfigSurfaceExposesOnlyNeutralPolicyTypes() {
        RecordComponent[] components = OtlpExporterConfig.class.getRecordComponents();
        assertThat(componentNamed(components, "retry").getType())
                .as("OtlpExporterConfig.retry component type")
                .isEqualTo(RetryPolicy.class);
        assertThat(componentNamed(components, "timeout").getType())
                .as("OtlpExporterConfig.timeout component type")
                .isEqualTo(TimeoutPolicy.class);
        for (RecordComponent component : components) {
            assertThat(component.getType().getName())
                    .as("OtlpExporterConfig." + component.getName() + " component type")
                    .doesNotStartWith(RESILIENCE4J_PREFIX);
        }

        // The exporter config's own builder accepts the two neutral policies directly, by their
        // neutral types, and nothing resilience-library flavoured stands in their place.
        assertThatCode(() -> OtlpExporterConfig.Builder.class.getMethod("retryPolicy", RetryPolicy.class))
                .as("OtlpExporterConfig.Builder exposes retryPolicy(RetryPolicy)")
                .doesNotThrowAnyException();
        assertThatCode(
                        () -> OtlpExporterConfig.Builder.class.getMethod("timeoutPolicy", TimeoutPolicy.class))
                .as("OtlpExporterConfig.Builder exposes timeoutPolicy(TimeoutPolicy)")
                .doesNotThrowAnyException();

        // The exporter entry-point builder exposes the same neutral retry policy setter, by its neutral
        // type; it does not widen or replace it with a resilience library type.
        assertThatCode(() -> OtlpExporter.Builder.class.getMethod("retryPolicy", RetryPolicy.class))
                .as("OtlpExporter.Builder exposes retryPolicy(RetryPolicy)")
                .doesNotThrowAnyException();
    }

    @DisplayName("the transport carries no transitive Resilience4j module requirement")
    @Test
    void transportHasNoTransitiveResilience4jRequirement() {
        assertNoTransitiveResilience4jRequirement(OtlpExporter.class);
    }

    @DisplayName("the transport entry points carry no transitive Resilience4j module requirement")
    @Test
    void transportEntryPointsHaveNoTransitiveResilience4jRequirement() {
        assertNoTransitiveResilience4jRequirement(OtlpExporter.class);
    }

    private static void assertNoTransitiveResilience4jRequirement(Class<?> representative) {
        ModuleDescriptor descriptor = readModuleDescriptor(representative);
        List<String> offenders = new ArrayList<>();
        for (ModuleDescriptor.Requires requires : descriptor.requires()) {
            boolean transitive =
                    requires.modifiers().contains(ModuleDescriptor.Requires.Modifier.TRANSITIVE);
            if (transitive && requires.name().startsWith(RESILIENCE4J_PREFIX)) {
                offenders.add(requires.name());
            }
        }
        assertThat(offenders)
                .as(descriptor.name() + " transitive requires directives naming a Resilience4j module")
                .isEmpty();
    }

    // ----- Reflection over public constructors, methods, and fields -----

    private static void collectResilience4jSignatures(Class<?> type, List<String> offenders) {
        for (Constructor<?> constructor : type.getConstructors()) {
            recordIfNamesResilience4j(
                    type, "constructor " + constructor, offenders, constructor.getParameterTypes());
            recordIfNamesResilience4j(
                    type, "constructor " + constructor, offenders, constructor.getExceptionTypes());
        }
        for (Method method : type.getMethods()) {
            if (method.getDeclaringClass() == Object.class) {
                continue;
            }
            recordIfNamesResilience4j(
                    type, "method " + method, offenders, new Class<?>[] {method.getReturnType()});
            recordIfNamesResilience4j(type, "method " + method, offenders, method.getParameterTypes());
            recordIfNamesResilience4j(type, "method " + method, offenders, method.getExceptionTypes());
        }
        for (Field field : type.getFields()) {
            recordIfNamesResilience4j(
                    type, "field " + field, offenders, new Class<?>[] {field.getType()});
        }
    }

    private static void recordIfNamesResilience4j(
            Class<?> owner, String member, List<String> offenders, Class<?>[] candidates) {
        for (Class<?> candidate : candidates) {
            Class<?> element = candidate;
            while (element.isArray()) {
                element = element.getComponentType();
            }
            if (element.getName().startsWith(RESILIENCE4J_PREFIX)) {
                offenders.add(owner.getName() + " " + member + " names " + element.getName());
            }
        }
    }

    private static RecordComponent componentNamed(RecordComponent[] components, String name) {
        for (RecordComponent component : components) {
            if (component.getName().equals(name)) {
                return component;
            }
        }
        throw new AssertionError("OtlpExporterConfig has no record component named " + name);
    }

    // ----- Classpath scanning of the exported packages -----

    /// Lists every class declared directly in `packageName` on the current test classpath (top level
    /// and nested, any visibility), loading each so its declared members can be reflected over.
    /// Supports both a `target/classes`-style directory classpath entry, the shape a reactor build
    /// takes, and a packaged jar, the shape a resolved build takes.
    private static List<Class<?>> classesInPackage(String packageName) {
        String path = packageName.replace('.', '/');
        ClassLoader loader = ExporterRuntimeNegativeSpaceTest.class.getClassLoader();
        List<Class<?>> found = new ArrayList<>();
        try {
            Enumeration<URL> resources = loader.getResources(path);
            while (resources.hasMoreElements()) {
                URL resource = resources.nextElement();
                switch (resource.getProtocol()) {
                    case "file" -> found.addAll(classesInDirectory(new File(resource.toURI()), packageName));
                    case "jar" -> found.addAll(classesInJar(resource, packageName));
                    default -> throw new IllegalStateException(
                            "unsupported classpath entry protocol for package " + packageName + ": " + resource);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
        return found;
    }

    private static List<Class<?>> classesInDirectory(File directory, String packageName) {
        List<Class<?>> found = new ArrayList<>();
        File[] files = directory.listFiles();
        if (files == null) {
            return found;
        }
        for (File file : files) {
            if (file.isFile() && file.getName().endsWith(".class") && !isCompilerPseudoClass(file.getName())) {
                String simple = file.getName().substring(0, file.getName().length() - ".class".length());
                found.add(loadClass(packageName + "." + simple));
            }
        }
        return found;
    }

    private static List<Class<?>> classesInJar(URL resource, String packageName) throws IOException {
        JarURLConnection connection = (JarURLConnection) resource.openConnection();
        File jarFile;
        try {
            jarFile = new File(connection.getJarFileURL().toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
        String prefix = packageName.replace('.', '/') + "/";
        List<Class<?>> found = new ArrayList<>();
        try (JarFile jar = new JarFile(jarFile)) {
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();
                if (entry.isDirectory()
                        || !name.startsWith(prefix)
                        || !name.endsWith(".class")
                        || name.indexOf('/', prefix.length()) >= 0) {
                    continue;
                }
                String fileName = name.substring(prefix.length());
                if (isCompilerPseudoClass(fileName)) {
                    continue;
                }
                String simple = fileName.substring(0, fileName.length() - ".class".length());
                found.add(loadClass(packageName + "." + simple));
            }
        }
        return found;
    }

    /// `package-info.class` is a compiler generated placeholder carrying package level annotations, not
    /// a real type; it is excluded from the scan rather than loaded and reflected over.
    private static boolean isCompilerPseudoClass(String classFileName) {
        return "package-info.class".equals(classFileName);
    }

    private static Class<?> loadClass(String binaryName) {
        try {
            return Class.forName(binaryName, false, ExporterRuntimeNegativeSpaceTest.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("failed to load " + binaryName + " off the test classpath", e);
        }
    }

    // ----- Code source resolution shared by the whole-module scan and the descriptor guard -----

    /// The code source root of `representative`'s module: a `target/classes` directory in the reactor
    /// build, or a jar file in a resolved build.
    private static File codeSourceLocation(Class<?> representative) {
        try {
            URL location = representative.getProtectionDomain().getCodeSource().getLocation();
            return new File(location.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    /// Every `.class` file name (its file name, including any nested class dollar marker) that the
    /// module contributing `classesRoot` carries, regardless of package, so this scan reaches internal
    /// packages too, not only the exported surface.
    private static List<String> allClassFileNames(File classesRoot) {
        List<String> names = new ArrayList<>();
        if (classesRoot.isDirectory()) {
            try (Stream<Path> walk = Files.walk(classesRoot.toPath())) {
                walk.filter(p -> p.getFileName().toString().endsWith(".class"))
                        .forEach(p -> names.add(p.getFileName().toString()));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return names;
        }
        try (JarFile jar = new JarFile(classesRoot)) {
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                if (!entry.isDirectory() && entry.getName().endsWith(".class")) {
                    String name = entry.getName();
                    int slash = name.lastIndexOf('/');
                    names.add(slash >= 0 ? name.substring(slash + 1) : name);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return names;
    }

    private static String simpleNameOf(String classFileName) {
        String withoutExtension = classFileName.substring(0, classFileName.length() - ".class".length());
        int dollar = withoutExtension.lastIndexOf('$');
        return dollar >= 0 ? withoutExtension.substring(dollar + 1) : withoutExtension;
    }

    /// Reads `representative`'s module's compiled `module-info.class` directly off its code source
    /// location, the resolution described in this class's javadoc.
    private static ModuleDescriptor readModuleDescriptor(Class<?> representative) {
        File root = codeSourceLocation(representative);
        try {
            if (root.isDirectory()) {
                File descriptorFile = new File(root, "module-info.class");
                try (InputStream in = new FileInputStream(descriptorFile)) {
                    return ModuleDescriptor.read(in);
                }
            }
            try (JarFile jar = new JarFile(root)) {
                JarEntry entry = jar.getJarEntry("module-info.class");
                if (entry == null) {
                    throw new IllegalStateException("no module-info.class found at " + root);
                }
                try (InputStream in = jar.getInputStream(entry)) {
                    return ModuleDescriptor.read(in);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
