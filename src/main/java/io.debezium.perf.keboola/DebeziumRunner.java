package io.debezium.perf.keboola;

import io.debezium.config.Configuration;
import io.debezium.embedded.Connect;
import io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

class DebeziumRunner {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final DebeziumEngine<?> engine;

    public DebeziumRunner(Configuration config, DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> consumer) {
        var format = KeyValueHeaderChangeEventFormat.of(Connect.class, Connect.class, Connect.class);
        engine = DebeziumEngine.create(format, ConvertingAsyncEngineBuilderFactory.class.getName())
                .using(config.asProperties())
                .notifying(consumer)
                .build();
    }

    public void start() {
        System.out.println(">> Starting Debezium engine");
        executor.submit(engine);
    }

    public void runSnapshot() throws InterruptedException, IOException {
        start();
        // TODO: this triggers once the Engine stops sending records, but there are still batches that are in-flight
        //  and those get lost if we .close engine
        StateMonitor.STATE.awaitSnapshotCompleted();
        stop();
    }

    public void stop() throws IOException {
        try {
            System.out.println(">> Stopping Debezium engine");
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
            engine.close(); // basically a hack to only call close 10 seconds after the engine stops sending records :shrug:
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
