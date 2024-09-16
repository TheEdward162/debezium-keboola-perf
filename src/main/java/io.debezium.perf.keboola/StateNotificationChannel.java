package io.debezium.perf.keboola;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.notification.SnapshotStatus;
import io.debezium.pipeline.notification.channels.NotificationChannel;

import static io.debezium.pipeline.notification.InitialSnapshotNotificationService.INITIAL_SNAPSHOT;

public class StateNotificationChannel implements NotificationChannel {
    @Override
    public void init(CommonConnectorConfig config) {
        // no-op
    }

    @Override
    public String name() {
        return "state-notification-channel";
    }

    @Override
    public void send(Notification notification) {
        var aggregateType = notification.getAggregateType();
        var type = notification.getType();

        if (aggregateType.equals(INITIAL_SNAPSHOT)) {
            notifySnapshot(type);
        }
    }

    public void notifySnapshot(String type) {
        var received = SnapshotStatus.valueOf(type);
        switch (received) {
            case SnapshotStatus.STARTED -> StateMonitor.STATE.snapshotStarted();
            case SnapshotStatus.COMPLETED -> StateMonitor.STATE.snapshotCompleted();
        }
    }

    @Override
    public void close() {
        // no-op
    }
}
