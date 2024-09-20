package io.debezium.perf.keboola;

import io.debezium.data.Uuid;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBColumnType;
import org.duckdb.DuckDBConnection;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class DuckDbWrapper {
	private final DuckDBConnection conn;
	private final String dbPath;
	private final String tempDir;
	private final int maxThreads;
	private final String memoryLimit;
	private final String maxMemory;

	public DuckDbWrapper(
		String dbPath,
		String tempDir,
		int maxThreads,
		String memoryLimit,
		String maxMemory
	) {
		this.dbPath = dbPath;
		this.tempDir = tempDir;
		this.maxThreads = maxThreads;
		this.memoryLimit = memoryLimit;
		this.maxMemory = maxMemory;
		try {
			// Load the DuckDB JDBC driver
			Class.forName("org.duckdb.DuckDBDriver");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}

		// Establish a connection to the DuckDB database
		try {
			this.conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:" + this.dbPath);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		init();
	}

	private void init() {
		try (var stmt = this.conn.createStatement()) {
			// Create a Statement object for sending SQL statements to the DB

			// Set the temporary directory for DuckDB
			stmt.execute("PRAGMA temp_directory='" + this.tempDir + "'");

			// Set the number of threads that DuckDB can use for parallel execution
			stmt.execute("PRAGMA threads=" + this.maxThreads);

			// Set the maximum amount of memory that DuckDB can use
			stmt.execute("PRAGMA memory_limit='" + this.memoryLimit + "'");

			// Set the maximum amount of memory that DuckDB can use for temporary data storage
			stmt.execute("PRAGMA max_memory='" + this.maxMemory + "'");
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

	public DuckDBAppender createTableAppender(String tableName, List<SchemaElement> fields) {
		final String columnDefinition = fields.stream()
				.map(f -> MessageFormat.format("\"{0}\" {1}", f.field(), dbType(f)))
				.collect(Collectors.joining(", "));

		try (final var stmt = this.conn.createStatement()) {
			stmt.execute("CREATE TABLE IF NOT EXISTS \"" + tableName + "\" (" + columnDefinition + ")");
			return this.conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, tableName);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static void appendValue(DuckDBAppender appender, SchemaElement field, Object value) throws SQLException {
		if (field.orderEvent()) {
			appender.append((int)value);
		} else if (value == null) {
			appender.append(null);
		} else if (field.isDate()) {
			var val = LocalDate.ofEpochDay((int)value).format(DateTimeFormatter.ISO_LOCAL_DATE);
			appender.append(val);
		} else if (field.isTimestamp()) {
			appender.appendLocalDateTime(LocalDateTime.ofInstant(Instant.ofEpochMilli((long)value), ZoneOffset.UTC));
		} else {
			appender.append(value.toString()); // TODO: needs better serialization
		}
	}

	private static String dbType(SchemaElement field) {
		return switch (field.type()) {
			case "int", "int16", "int32" -> {
				if (field.isDate()) {
					yield DuckDBColumnType.DATE.name();
				}
				yield DuckDBColumnType.INTEGER.name();
			}
			case "int64" -> {
				if (field.isTimestamp()) {
					yield DuckDBColumnType.TIMESTAMP.name();
				}
				yield DuckDBColumnType.BIGINT.name();
			}
			case "timestamp" -> DuckDBColumnType.TIMESTAMP.name();
			case "string" -> {
				if (Objects.equals(field.name(), Uuid.LOGICAL_NAME)) {
					yield DuckDBColumnType.UUID.name();
				}
				if (field.isZonedTimestamp()) {
					yield "timestamptz";
				}
				if (field.isInterval()) {
					yield DuckDBColumnType.INTERVAL.name();
				}
				yield DuckDBColumnType.VARCHAR.name();
			}
			case "bytes", "array", "struct" -> DuckDBColumnType.VARCHAR.name();
            case "boolean" -> DuckDBColumnType.BOOLEAN.name();
			case "float" -> DuckDBColumnType.FLOAT.name();
			case "double" -> DuckDBColumnType.DOUBLE.name();
			case "date" -> DuckDBColumnType.DATE.name();
			case "time" -> DuckDBColumnType.TIME.name();
			default -> throw new IllegalArgumentException("Unknown type: " + field.type());
		};
	}
}
