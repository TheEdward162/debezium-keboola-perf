package io.debezium.perf.keboola;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.*;
import java.util.List;
import java.util.Objects;

public class TestConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {
    private final Writer outFile;

    public TestConsumer() {
        try {
            this.outFile = new OutputStreamWriter(new FileOutputStream("data/out.txt"));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<SourceRecord, SourceRecord>> records, DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer)
            throws InterruptedException {
        for (final var r : records) {
            try {
                final var key = Objects.toString(r.key());
                final var value = Objects.toString(r.value());
                this.outFile.write(key);
                this.outFile.write(':');
                this.outFile.write(value);
                this.outFile.write('\n');
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            committer.markProcessed(r);
        }
        committer.markBatchFinished();
    }
}
