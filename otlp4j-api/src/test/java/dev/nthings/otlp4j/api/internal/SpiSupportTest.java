package dev.nthings.otlp4j.api.internal;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.nthings.otlp4j.spi.OtlpServerProvider;
import org.junit.jupiter.api.Test;

/// Unit tests for the internal `SpiSupport` service locator. Lives in the non-exported
/// `dev.nthings.otlp4j.api.internal` package so it can reach the class directly.
///
/// The `otlp4j-api` test runtime has no transport provider on its module path, so
/// resolving a transport SPI here exercises the missing-provider path.
class SpiSupportTest {

    @Test
    void throwsAHelpfulErrorWhenNoProviderIsRegistered() {
        assertThatThrownBy(() -> SpiSupport.provider(OtlpServerProvider.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No OtlpServerProvider found")
                .hasMessageContaining("otlp4j-transport");
    }
}
