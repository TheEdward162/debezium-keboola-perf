package io.debezium.perf.keboola;

import io.debezium.time.Date;
import io.debezium.time.Interval;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;

import java.util.Objects;

public record SchemaElement(
        String type,
        boolean optional,
        String defaultValue,
        String name,
        Integer version,
        String field,
        boolean orderEvent
) {
    public boolean isDate() {
        return Objects.equals(this.name, Date.SCHEMA_NAME) || Objects.equals(this.name, org.apache.kafka.connect.data.Date.LOGICAL_NAME);
    }

    public boolean isTimestamp() {
        return Objects.equals(this.name, Timestamp.SCHEMA_NAME) || Objects.equals(this.name, org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME);
    }

    public boolean isInterval() {
        return Objects.equals(this.name, Interval.SCHEMA_NAME);
    }

    public boolean isZonedTimestamp() {
        return Objects.equals(this.name, ZonedTimestamp.SCHEMA_NAME);
    }
}