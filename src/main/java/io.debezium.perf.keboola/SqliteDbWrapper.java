package io.debezium.perf.keboola;

import org.sqlite.SQLiteConnection;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

public class SqliteDbWrapper {
    public static class SqliteDbAppender {
        private final SQLiteConnection conn;
        private final PreparedStatement preparedStatement;

        private int fieldIndex; // TODO: should append look up the column index instead?

        private SqliteDbAppender(SQLiteConnection conn, PreparedStatement preparedStatement) {
            this.preparedStatement = preparedStatement;
            this.conn = conn;
        }

        public void beginRow() {
            this.fieldIndex = 1;
        }

        public void append(SchemaElement field, Object value) throws SQLException {
            if (field.orderEvent()) {
                this.preparedStatement.setInt(this.fieldIndex, (Integer) value);
            } else if (value == null) {
                this.preparedStatement.setNull(this.fieldIndex, dbType(field).type);
            } else if (field.isDate()) {
                final var val = LocalDate.ofEpochDay((int)value).format(DateTimeFormatter.ISO_LOCAL_DATE);
                this.preparedStatement.setString(this.fieldIndex, val);
            } else if (field.isTimestamp()) {
                final var val = LocalDateTime.ofInstant(Instant.ofEpochMilli((long)value), ZoneOffset.UTC);
                this.preparedStatement.setString(this.fieldIndex, val.toString());
            } else {
                this.preparedStatement.setString(this.fieldIndex, value.toString()); // TODO: needs better serialization
            }

            this.fieldIndex += 1;
        }

        public void endRow() throws SQLException {
            // TODO: or we could use executeBatch?
            this.preparedStatement.execute();
            this.preparedStatement.clearParameters();
        }

        public void flush() throws SQLException {
            this.conn.commit();
        }

        public void close() {}
    }

    private final Path dbPath;
    private final SQLiteConnection conn;

    public SqliteDbWrapper(
            Path dbPath
    ) {
        this.dbPath = dbPath;

        try {
            this.conn = (SQLiteConnection) DriverManager.getConnection("jdbc:sqlite:" + this.dbPath);
            this.conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            this.conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public SqliteDbAppender createTableAppender(String tableName, List<SchemaElement> fields) {
        final String columnDefinition = fields.stream()
                .map(f -> MessageFormat.format("\"{0}\" {1}", f.field(), dbType(f).name))
                .collect(Collectors.joining(", "));
        final String columnNames = fields.stream()
                .map(f -> MessageFormat.format("\"{0}\"", f.field()))
                .collect(Collectors.joining(", "));
        final String columnParameters = fields.stream()
                .map(f -> "?")
                .collect(Collectors.joining(", "));

        try (final var stmt = this.conn.createStatement()) {
            stmt.execute(MessageFormat.format("CREATE TABLE IF NOT EXISTS \"{0}\" ({1})", tableName, columnDefinition));
            final var preparedStatement = this.conn.prepareStatement(MessageFormat.format(
                    "INSERT INTO \"{0}\" ({1}) VALUES ({2})",
                    tableName, columnNames, columnParameters
            ));

            return new SqliteDbAppender(this.conn, preparedStatement);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private record DbType(
            int type,
            String name
    ) {}
    private static DbType dbType(SchemaElement field) {
        return switch (field.type()) {
            case "int", "int16", "int32" -> {
                if (field.isDate()) {
                    yield new DbType(Types.VARCHAR, "TEXT");
                }
                yield new DbType(Types.INTEGER, "INTEGER");
            }
            case "int64"  -> new DbType(Types.BIGINT, "BIGINT");
            case "string", "bytes", "array", "struct", "date", "time", "timestamp" -> new DbType(Types.VARCHAR, "TEXT");
            case "boolean" -> new DbType(Types.BOOLEAN, "BOOLEAN");
            case "float" -> new DbType(Types.REAL, "REAL");
            case "double" -> new DbType(Types.DOUBLE, "DOUBLE");
            default -> throw new IllegalArgumentException("Unknown type: " + field.type());
        };
    }
}
