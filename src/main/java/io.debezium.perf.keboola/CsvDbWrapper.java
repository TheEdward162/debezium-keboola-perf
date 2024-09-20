package io.debezium.perf.keboola;

import com.opencsv.CSVWriter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class CsvDbWrapper {
    public static class CsvDbAppender {
        private final CSVWriter stream;
        private List<String> row;

        private CsvDbAppender(CSVWriter stream) {
            this.stream = stream;
            this.row = new ArrayList<String>();
        }

        public void beginRow() {
            this.row.clear();
        }

        public void append(SchemaElement field, Object value) {
            if (field.orderEvent()) {
                this.row.add(value.toString()); // int
            } else if (value == null) {
                this.row.add("");
            } else if (field.isDate()) {
                final var val = LocalDate.ofEpochDay((int)value).format(DateTimeFormatter.ISO_LOCAL_DATE);
                this.row.add(val);
            } else if (field.isTimestamp()) {
                final var val = LocalDateTime.ofInstant(Instant.ofEpochMilli((long)value), ZoneOffset.UTC);
                this.row.add(val.toString());
            } else {
                this.row.add(value.toString()); // TODO: needs better serialization
            }
        }

        public void endRow() throws IOException {
            this.stream.writeNext(this.row.toArray(new String[0]));
        }

        public void flush() throws IOException {
            this.stream.flush();
        }

        public void close() throws IOException {
            this.stream.close();
        }
    }

    private final Path basePath;
    public CsvDbWrapper(Path basePath) {
        this.basePath = basePath;
        try {
            Files.createDirectory(this.basePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CsvDbAppender createTableAppender(String tableName, List<SchemaElement> fields) {
        try {
            final var stream = new CSVWriter(
                    new FileWriter(this.basePath.resolve(tableName + ".csv").toFile())
            );
            final var header = fields.stream()
                    .map(SchemaElement::field)
                    .toList().toArray(new String[0]);
            stream.writeNext(header);

            return new CsvDbAppender(stream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        // no-op, for parity with DuckDBWrapper
    }
}
