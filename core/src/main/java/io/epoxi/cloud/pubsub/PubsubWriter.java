package io.epoxi.cloud.pubsub;

import java.lang.invoke.MethodHandles;
import org.slf4j.Marker;
import io.epoxi.cloud.logging.AppLog;

public class PubsubWriter{

    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

    private final PubsubTopic topic;

    private PubsubWriter(PubsubTopic topic)
    {
        Marker marker = logger.logEntry("Constructor");

        this.topic = topic;
        logger.logExit(marker);
    }

    public static PubsubWriter create(PubsubTopic topic)
    {
        return new PubsubWriter(topic);
    }

    public void write(Writable[] writables) throws InterruptedException {

        for(Writable writable : writables)
        {
            write(writable);
        }
        logger.atDebug().log("Writer has published {} event to the topic '{}'", writables.length, topic.getTopicName().getTopic());
    }

    public void write(Writable writable) throws  InterruptedException {

        topic.publishMessage(writable.toMessage());

    }
}
