package dev.nthings.otlp4j.bench;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import dev.nthings.otlp4j.codec.SignalCodec;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/// Isolates pure OTLP request marshalling cost.
///
/// [#parseDecodeEncodeSerialize] parses a proto export request from wire bytes, decodes it to the
/// domain model, re-encodes the domain model back to a proto request, and serializes it to bytes. The
/// operation starts from wire bytes and has no socket or thread involved. [#parseSerialize] parses the
/// same wire bytes and serializes the parsed proto message straight back with no domain step in
/// between, representing a same-protocol relay. Both benchmarks parameterize on signal (traces,
/// metrics, logs) and payload size (small and large).
@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class CodecMarshallingBenchmark {

    /// The signal whose codec is exercised.
    @Param({"TRACES", "METRICS", "LOGS"})
    public String signal;

    /// The payload size label; see [BenchmarkPayloads#itemCount(String)].
    @Param({"small", "large"})
    public String size;

    private SignalCodec<?, ?, ?> codec;
    private byte[] requestBytes;

    /// Selects the codec for the chosen signal and pre-serializes the request bytes for the chosen
    /// size so the measured operation starts from wire bytes.
    @Setup(Level.Trial)
    public void setUp() {
        int items = BenchmarkPayloads.itemCount(size);
        switch (signal) {
            case "TRACES" -> {
                codec = SignalCodec.TRACES;
                requestBytes = SignalCodec.TRACES.toProto(BenchmarkPayloads.traces(items)).toByteArray();
            }
            case "METRICS" -> {
                codec = SignalCodec.METRICS;
                requestBytes = SignalCodec.METRICS.toProto(BenchmarkPayloads.metrics(items)).toByteArray();
            }
            case "LOGS" -> {
                codec = SignalCodec.LOGS;
                requestBytes = SignalCodec.LOGS.toProto(BenchmarkPayloads.logs(items)).toByteArray();
            }
            default -> throw new IllegalArgumentException("unknown signal: " + signal);
        }
    }

    /// Parses, decodes, re-encodes, and serializes one request through the immutable domain model.
    ///
    /// @return the re-serialized request bytes
    /// @throws InvalidProtocolBufferException if the pre-built bytes fail to parse
    @Benchmark
    public byte[] parseDecodeEncodeSerialize() throws InvalidProtocolBufferException {
        return marshalRoundTrip(codec, requestBytes);
    }

    /// Parses one request and serializes the parsed proto message straight back, with no domain
    /// materialisation.
    ///
    /// @return the re-serialized request bytes
    /// @throws InvalidProtocolBufferException if the pre-built bytes fail to parse
    /// @throws IOException                    if the write to the fixed buffer fails
    @Benchmark
    public byte[] parseSerialize() throws InvalidProtocolBufferException, IOException {
        return parseThenWrite(codec, requestBytes);
    }

    private static <T, R extends MessageLite, S extends MessageLite> byte[] marshalRoundTrip(
            SignalCodec<T, R, S> codec, byte[] bytes) throws InvalidProtocolBufferException {
        R request = codec.parseRequest(bytes);
        T domain = codec.toDomain(request);
        R reencoded = codec.toProto(domain);
        return reencoded.toByteArray();
    }

    private static <T, R extends MessageLite, S extends MessageLite> byte[] parseThenWrite(
            SignalCodec<T, R, S> codec, byte[] bytes) throws InvalidProtocolBufferException, IOException {
        R request = codec.parseRequest(bytes);
        var body = new byte[request.getSerializedSize()];
        request.writeTo(new FixedBufferOutputStream(body));
        return body;
    }

    /// A fixed-capacity `OutputStream` over a preallocated, presized array, mirroring the direct-write
    /// path `HttpOtlpClient` uses at the transport send boundary. Writing into a buffer sized exactly
    /// to `getSerializedSize()` costs exactly one array allocation, unlike `ByteArrayOutputStream`,
    /// whose `toByteArray()` always returns a fresh defensive copy.
    private static final class FixedBufferOutputStream extends OutputStream {

        private final byte[] buffer;
        private int position;

        FixedBufferOutputStream(byte[] buffer) {
            this.buffer = buffer;
        }

        @Override
        public void write(int b) {
            buffer[position++] = (byte) b;
        }

        @Override
        public void write(byte[] b, int off, int len) {
            System.arraycopy(b, off, buffer, position, len);
            position += len;
        }
    }
}
