package dev.nthings.otlp4j.bench;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import dev.nthings.otlp4j.codec.SignalCodec;
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

/// Isolates the pure marshalling cost the Phase 2 payload redesign targets.
///
/// One measured operation parses a proto export request from wire bytes, decodes it to the domain
/// model, re-encodes the domain model back to a proto request, and serializes it to bytes. That is
/// exactly the parse plus toDomain plus toProto plus serialize round trip a transport performs per
/// request, with no socket or thread involved, so a Phase 2 improvement can be attributed to the
/// codec rather than the transport. It parameterizes on signal (traces, metrics, logs) and payload
/// size (small and large).
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

    /// Parses, decodes, re-encodes, and serializes one request.
    ///
    /// @return the re-serialized request bytes
    /// @throws InvalidProtocolBufferException if the pre-built bytes fail to parse
    @Benchmark
    public byte[] parseDecodeEncodeSerialize() throws InvalidProtocolBufferException {
        return marshalRoundTrip(codec, requestBytes);
    }

    private static <T, R extends MessageLite, S extends MessageLite> byte[] marshalRoundTrip(
            SignalCodec<T, R, S> codec, byte[] bytes) throws InvalidProtocolBufferException {
        R request = codec.parseRequest(bytes);
        T domain = codec.toDomain(request);
        R reencoded = codec.toProto(domain);
        return reencoded.toByteArray();
    }
}
