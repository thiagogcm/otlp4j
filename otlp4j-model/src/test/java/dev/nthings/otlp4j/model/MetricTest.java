package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Metric data helpers")
class MetricTest {

    @DisplayName("hasData()/dataOrThrow() expose a present data payload")
    @Test
    void presentData() {
        var data = new Metric.Gauge(List.of());
        var metric = Metric.builder().name("m").data(data).build();

        assertThat(metric.hasData()).isTrue();
        assertThat(metric.dataOrThrow()).isSameAs(data);
    }

    @DisplayName("hasData() is false and dataOrThrow() throws for the DATA_NOT_SET form")
    @Test
    void absentData() {
        // data defaults to NoData (the wire DATA_NOT_SET case).
        var metric = Metric.builder().name("m").build();

        assertThat(metric.hasData()).isFalse();
        assertThat(metric.data()).isEqualTo(Metric.NoData.INSTANCE);
        assertThatThrownBy(metric::dataOrThrow)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("m")
                .hasMessageContaining("DATA_NOT_SET");
    }

    @DisplayName("the record rejects null data")
    @Test
    void rejectsNullData() {
        assertThatThrownBy(() -> new Metric("m", "", "", null, Attributes.empty()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Metric.builder().data(null))
                .isInstanceOf(NullPointerException.class);
    }

    @DisplayName("the record rejects null required metadata")
    @Test
    void rejectsNullRequiredMetadata() {
        var data = new Metric.Gauge(List.of());

        assertThatThrownBy(() -> new Metric(null, "", "", data, Attributes.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("name");
        assertThatThrownBy(() -> new Metric("m", null, "", data, Attributes.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("description");
        assertThatThrownBy(() -> new Metric("m", "", null, data, Attributes.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("unit");
        assertThatThrownBy(() -> new Metric("m", "", "", data, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("metadata");
    }

    @DisplayName("metric data records reject null temporality")
    @Test
    void rejectsNullTemporality() {
        assertThatThrownBy(() -> new Metric.Sum(List.of(), null, true))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("temporality");
        assertThatThrownBy(() -> new Metric.Histogram(List.of(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("temporality");
        assertThatThrownBy(() -> new Metric.ExponentialHistogram(List.of(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("temporality");
    }
}
