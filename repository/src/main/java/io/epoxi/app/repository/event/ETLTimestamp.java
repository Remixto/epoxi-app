package io.epoxi.app.repository.event;

import com.google.cloud.Timestamp;

public class ETLTimestamp {

    final Timestamp timestamp;

    private ETLTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public long toNanos() {
        long l = timestamp.toSqlTimestamp().getTime();
        return l * 1000;
    }

    public Timestamp toTimestamp() {
        return timestamp;
    }

    public static ETLTimestamp now() {
        return new ETLTimestamp(Timestamp.now());
    }

    public static ETLTimestamp fromNanos(Long nanoseconds) {
        long seconds = nanoseconds / 1000000;
        long nanos = nanoseconds - (seconds * 1000000);

        Timestamp t = Timestamp.ofTimeSecondsAndNanos(seconds, (int) nanos);
        return new ETLTimestamp(t);
    }

    public static ETLTimestamp fromTimestamp(String t) {
        return new ETLTimestamp(Timestamp.parseTimestamp(t));
    }

    public static ETLTimestamp fromTimestamp(Timestamp t) {
        return new ETLTimestamp(t);
    }

}