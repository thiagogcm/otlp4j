package dev.nthings.otlp4j.components.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SizerTest {

    @Test
    void itemsReadsTheItemTotal() {
        assertThat(Sizer.ITEMS.read(7, 999)).isEqualTo(7);
    }

    @Test
    void bytesReadsTheByteTotal() {
        assertThat(Sizer.BYTES.read(7, 999)).isEqualTo(999);
    }
}
