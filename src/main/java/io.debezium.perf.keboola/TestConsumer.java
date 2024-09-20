package io.debezium.perf.keboola;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;

import java.io.IOException;
import java.io.StringReader;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


public class TestConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>> {
    private static final SchemaElement ORDER_EVENT = new SchemaElement("int", false, null, null, 1, "kbc__batch_event_order", true);
    private static final Gson GSON = new Gson();

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
    public void handleBatch(List<ChangeEvent<String, String>> records, DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {
        for (final var r : records) {
            final var value =  extractRecordValue(r.value());

            value.fields.add(ORDER_EVENT);
            final var appenderState = this.appenderForTable(value.tableName, value.fields());
            value.values.add(appenderState.sequence.getAndIncrement());

            try {
                appenderState.appender.beginRow();
                for (int i = 0; i < value.fields().size(); i += 1) {
                    appenderState.appender.append(value.fields().get(i), value.values().get(i));
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

    private record RecordValue(
            String tableName,
            List<SchemaElement> fields,
            List<Object> values
    ) {}
    private static RecordValue extractRecordValue(String value) {
        String tableName = "";
        List<SchemaElement> fields = new ArrayList<>();
        JsonObject payload = null;

        JsonReader reader = new JsonReader(new StringReader(value));
        try {
            reader.beginObject();

            while (reader.hasNext()) {
                var nextName = reader.nextName();
                if ("payload".equals(nextName)) {
                    payload = JsonParser.parseReader(reader).getAsJsonObject();
                } else if ("schema".equals(nextName)) {
                    reader.beginObject();
                    while (reader.hasNext()) {
                        nextName = reader.nextName();
                        if ("name".equals(nextName)) {
                            tableName = reader.nextString().replace(".Value", "");
                        } else if ("fields".equals(nextName)) {
                            reader.beginArray();
                            while (reader.hasNext()) {
                                fields.add(GSON.fromJson(reader, SchemaElement.class));
                            }
                            reader.endArray();
                        } else {
                            reader.skipValue(); // ignore other fields
                        }
                    }
                    reader.endObject();
                } else {
                    reader.skipValue(); // ignore other fields
                }
            }
            reader.endObject();
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final var values = new ArrayList<>();
        for (final var f : fields) {
            values.add(Objects.requireNonNull(payload).get(f.field()));
        }
        return new RecordValue(tableName, fields, values);
    }
}
