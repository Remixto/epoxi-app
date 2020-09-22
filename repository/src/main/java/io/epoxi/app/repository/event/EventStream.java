package io.epoxi.app.repository.event;

import io.epoxi.app.cloud.logging.AppLog;
import io.epoxi.app.cloud.pubsub.PubsubWriter;
import io.epoxi.app.cloud.pubsub.Writable;
import org.slf4j.Marker;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

public class EventStream {
    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());
    
    private final List<Writable> events = new ArrayList<>();
    
    private PubsubWriter writer;

    public EventStream()
    {
        Marker marker = logger.logEntry("Constructor");
        logger.logExit(marker);
    }

    public List<Writable> getEvents()
    {
        return events;
    }

    public PubsubWriter getWriter()
    {
        return writer;
    }

    public void setWriter(PubsubWriter value)
    {
        writer = value;
    }
}