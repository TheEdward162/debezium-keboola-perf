package io.debezium.perf.keboola;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;

public enum StateMonitor {
    STATE;

    private final CountDownLatch snapshotCompletedLatch = new CountDownLatch(1);
    private Instant snapshotStartedTime;
    private Instant snapshotCompletedTime;

    public void awaitSnapshotCompleted() throws InterruptedException {
        snapshotCompletedLatch.await();
    }

    public void snapshotCompleted() {
        snapshotCompletedTime = Instant.now();
        snapshotCompletedLatch.countDown();
    }

    public void snapshotStarted() {
        snapshotStartedTime = Instant.now();
    }

    public Duration getSnapshotDuration() {
        return Duration.between(snapshotStartedTime, snapshotCompletedTime);
    }
}
