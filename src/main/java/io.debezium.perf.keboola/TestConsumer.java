package io.debezium.perf.keboola;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.apache.kafka.connect.source.SourceRecord;
import org.duckdb.DuckDBAppender;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

public class TestConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {
    private final DuckDbWrapper duckDb;
    private final DuckDBAppender appender;

    public TestConsumer(DuckDbWrapper duckDb) {
        this.duckDb = duckDb;
        this.appender = duckDb.createTableAppender("str VARCHAR, num INTEGER", "test_table");
    }

    @Override
    public void handleBatch(List<ChangeEvent<SourceRecord, SourceRecord>> records, DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer)
            throws InterruptedException {
        for (final var r : records) {
            final var value = Objects.toString(r.value());
            try {
                this.appender.beginRow();
                this.appender.append("r");
                this.appender.append(value.length());
                this.appender.endRow();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            committer.markProcessed(r);
        }
        committer.markBatchFinished();

        try {
            this.appender.flush();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
