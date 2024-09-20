package io.debezium.perf.keboola;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


public class TestConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {
    private static final SchemaElement ORDER_EVENT = new SchemaElement("int", false, null, null, 1, "kbc__batch_event_order", true);

    private record AppenderState(
            CsvDbWrapper.CsvDbAppender appender,
            AtomicInteger sequence
    ) {}

    private final CsvDbWrapper db;
    private final Map<String, AppenderState> appenders;
    private final AtomicInteger debug;

    public TestConsumer(CsvDbWrapper db) {
        this.db = db;
        this.appenders = new HashMap<>();
        this.debug = new AtomicInteger(0);
    }

    private AppenderState appenderForTable(String tableName, List<SchemaElement> fields) {
        if (!this.appenders.containsKey(tableName)) {
            this.appenders.put(tableName, new AppenderState(
                    this.db.createTableAppender(tableName, fields),
                    new AtomicInteger(0)
            ));
        }

        return this.appenders.get(tableName);
    }

    @Override
    public void handleBatch(List<ChangeEvent<SourceRecord, SourceRecord>> records, DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer) throws InterruptedException {
        for (final var r : records) {
            final var value = (Struct)r.value().value();
            final var tableName = extractTableName(value);

            final var payload = extractPayload(value);

            payload.fields.add(ORDER_EVENT);
            final var appenderState = this.appenderForTable(tableName, payload.fields());
            payload.values.add(appenderState.sequence.getAndIncrement());

            try {
                appenderState.appender.beginRow();
                for (int i = 0; i < payload.fields().size(); i += 1) {
                    appenderState.appender.append(payload.fields().get(i), payload.values().get(i));
                }
                appenderState.appender.endRow();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            committer.markProcessed(r);
        }

        try {
            for (final var state : this.appenders.values()) {
                state.appender.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.err.println(MessageFormat.format("Seen total of {0} records", this.debug.addAndGet(records.size())));
        committer.markBatchFinished();
    }

    public void close() {
        try {
            for (var state : this.appenders.values()) {
                state.appender.flush();
                state.appender.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.db.close();
    }

    private static String extractTableName(Struct value) {
        return value.schema().name(); // .replace(".Value", ""); // used for UNWRAP
    }

    private record Payload(
           List<SchemaElement> fields,
           List<Object> values
    ) {}
    private static Payload extractPayload(Struct value) {
        final var fields = new ArrayList<SchemaElement>();
        final var values = new ArrayList<Object>();

        for (var f : value.schema().fields()) {
            final var schema = f.schema();
            fields.add(new SchemaElement(
                    schema.type().getName(),
                    schema.isOptional(),
                    schema.defaultValue() != null ? schema.defaultValue().toString() : null,
                    schema.name(),
                    schema.version(),
                    f.name(),
                    false
            ));
            values.add(value.get(f));
        }

        return new Payload(fields, values);
    }
}
