package dev.nthings.otlp4j.opentelemetry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Copies SDK collections at the adapter boundary so a caller cannot mutate an in-flight export. */
final class ListCopy {

    private ListCopy() {}

    static <T> List<T> copy(Collection<T> values) {
        return List.copyOf(new ArrayList<>(values));
    }
}
