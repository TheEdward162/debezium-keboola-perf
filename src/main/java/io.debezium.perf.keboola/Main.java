package io.debezium.perf.keboola;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.embedded.EmbeddedEngineConfig;
import io.debezium.embedded.async.AsyncEmbeddedEngine;
import io.debezium.storage.file.history.FileSchemaHistory;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class Main {

    public static final Path OFFSET_FILE = Path.of("data/offsets.dat");
    public static final Path SCHEMA_FILE = Path.of("data/schema.dat");
    public static final Path DATABASE_CONFIG = Path.of("conf/database-local.properties");

    private static Properties loadProps(Path path) throws IOException {
        Properties props = new Properties();

        try(var is = Files.newInputStream(path)) {
            props.load(is);
        }

        return props;
    }

    public static void main(String[] args) throws IOException {
        Files.deleteIfExists(OFFSET_FILE);
        Files.deleteIfExists(SCHEMA_FILE);

        if (!Files.exists(DATABASE_CONFIG)) {
            throw new IllegalArgumentException("Database configuration file does not exist: " + DATABASE_CONFIG);
        }
        var connConfig = loadProps(DATABASE_CONFIG);

        var config = Configuration.from(connConfig)
                .edit()
                .with(EmbeddedEngineConfig.ENGINE_NAME, "keboola")
                // .with(AsyncEmbeddedEngine.RECORD_PROCESSING_ORDER, "UNORDERED")
                // .with(AsyncEmbeddedEngine.RECORD_PROCESSING_THREADS, THREADS_COUNT)
                // .with(EmbeddedEngineConfig.CONNECTOR_CLASS, MySqlConnector.class.getName())
                // .with(EmbeddedEngineConfig.OFFSET_STORAGE, FileOffsetBackingStore.class.getName())
                // .with(EmbeddedEngineConfig.OFFSET_STORAGE_FILE_FILENAME, OFFSET_FILE.toString())
                // .with(MySqlConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class.getName())
                // .with("schema.history.internal.file.filename", SCHEMA_FILE.toString())
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "keboola-cdc")
                // .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, "cdc.large_100m")
                // .with(MySqlConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL_ONLY)
                .with(MySqlConnectorConfig.SERVER_ID, 123)
                .with(MySqlConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "state-notification-channel")
                // .with(MySqlConnectorConfig.MAX_BATCH_SIZE, 10_000)
                // .with(MySqlConnectorConfig.MAX_QUEUE_SIZE, 40_000)
                .build();

        var duckDb = new DuckDbWrapper(
                "data/test.duckdb",
                "data/duckdb_tmp",
                1,
                "2GB",
                "1GB"
        );
        var runner = new DebeziumRunner(
                config,
                new TestConsumer(duckDb)
        );

        try {
            runner.runSnapshot();
            duckDb.close();
        } catch (InterruptedException e) {
            System.err.println("Interrupted while stopping Debezium engine: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Error stopping Debezium engine: " + e.getMessage());
        }

        // done
        var duration = StateMonitor.STATE.getSnapshotDuration();
        System.out.println(">> Debezium engine stopped after  " + duration.toSeconds() + "s");

    }
}
