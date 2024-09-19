package io.debezium.perf.keboola;

import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;

import java.sql.DriverManager;
import java.sql.SQLException;

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

	public DuckDBAppender createTableAppender(String columnDefinition, String tableName) {
		try (final var stmt = this.conn.createStatement()) {
			stmt.execute("CREATE TABLE IF NOT EXISTS \"" + tableName + "\" (" + columnDefinition + ")");
			return this.conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, tableName);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
