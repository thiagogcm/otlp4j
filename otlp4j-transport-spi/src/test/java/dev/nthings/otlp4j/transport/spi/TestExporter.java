package dev.nthings.otlp4j.transport.spi;

import dev.nthings.otlp4j.spi.OtlpClient;
import java.util.function.Consumer;

/// Concrete `AbstractOtlpExporter` over a supplied client, shared by the transport-spi tests. The
/// leak-reporter constructor lets a test observe the GC backstop without a logging backend.
final class TestExporter extends AbstractOtlpExporter {

    TestExporter(OtlpClient client) {
        super(client);
    }

    TestExporter(OtlpClient client, Consumer<String> leakReporter) {
        super(client, leakReporter);
    }
}
