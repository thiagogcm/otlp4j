package dev.nthings.otlp4j.transport.internal;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Transport shared plumbing")
class TransportsTest {

    @DisplayName("requireCompleteClientMutualTls rejects half-specified in-memory material")
    @Test
    void requireCompleteClientMutualTlsBytes() {
        var pem = new byte[] {1, 2, 3};
        // Both present or both absent is complete.
        Transports.requireCompleteClientMutualTls(pem, pem);
        Transports.requireCompleteClientMutualTls((byte[]) null, (byte[]) null);
        // Half-specified is rejected.
        assertThatThrownBy(() -> Transports.requireCompleteClientMutualTls(pem, (byte[]) null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Transports.requireCompleteClientMutualTls((byte[]) null, pem))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
