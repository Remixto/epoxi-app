package io.epoxi.app.repository.event;

import io.epoxi.app.cloud.logging.AppLog;
import io.epoxi.app.cloud.pubsub.Writable;
import io.epoxi.app.repository.model.Ingestion;
import io.epoxi.app.repository.model.KeysetId;

import com.google.pubsub.v1.PubsubMessage;

import org.slf4j.Marker;

import com.google.gson.FieldNamingStrategy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.ByteString;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;

import java.util.List;
import java.util.ArrayList;

public class Event implements Writable {

    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

    private final KeysetId keysetId;
    private ETLTimestamp eventTimestamp;
    private Ingestion.ReplicationType updateType = Ingestion.ReplicationType.FULL;
    private Boolean isRequeue;

    private List<EventKey> eventKeys = new ArrayList<>();

     /**
     * Create an event.  The timestamp of the event
     * will be set during the creation to the moment of its creation.
     * @param keysetId The keyset for the event
     */
    public Event(KeysetId keysetId)
    {
        this(keysetId, ETLTimestamp.now());
    }

    /**
     * Create an event with a specific timestamp
     * @param keysetId The keyset for the event
     * @param timestamp A specific timestamp set by the Event creator.
     */
    public Event(KeysetId keysetId, ETLTimestamp timestamp)
    {
        this.keysetId = keysetId;
        this.eventTimestamp = timestamp;

        logger.atDebug().addJson("event", toJson()).log("Event created");
    }

    public static Event fromJson(String json)
    {
        Marker marker = logger.logEntry("Constructor");

        GsonBuilder builder = new GsonBuilder();
        Gson gson = builder.create();
        Event event = gson.fromJson(json, Event.class);

        if (event.eventKeys ==null) event.eventKeys = new ArrayList<>();
        if (event.eventTimestamp ==null) event.eventTimestamp = ETLTimestamp.now();

        if (Boolean.FALSE.equals(event.isValid()))
        {
            //Warning
            logger.atWarn().addJson("constructor-json", json).log("Cannot create Event from json.");
            event = null;
        }
        else
        {
            logger.atDebug().addJson("constructor-json", json).log("Event created from Json with {} keys", event.eventKeys.size());
        }

        logger.logExit(marker);
        return event;
    }

    public String toJson() {

		GsonBuilder builder = new GsonBuilder();
		builder.setPrettyPrinting().serializeNulls();
		builder.setFieldNamingStrategy(new ClassFieldNamingStrategy());
		Gson gson = builder.create();
		return gson.toJson(this);
    }

    // Custom FieldNamingStrategy
    private static class ClassFieldNamingStrategy implements FieldNamingStrategy {
        @Override
       public String translateName(Field field) {
           return field.getName();
       }
    }

    public static Event fromMessage(PubsubMessage message)
    {
        String json = message.getData().toStringUtf8();
        return fromJson(json);
    }

    public PubsubMessage toMessage()
    {
        String messageJson = toJson();
        ByteString data = ByteString.copyFromUtf8(messageJson);
        return PubsubMessage.newBuilder().setData(data).build();
    }

    /**
     * Get the row key for each row within the event.  Return them as EventKey objects
     */
    public List<EventKey> getEventKeys() {
        return eventKeys;
    }

    /**
     * Get the row key for each row within the event.  Return them as a simple list of Longs
     */
    public List<Long> getKeys() {
        List<Long> keys = new ArrayList<>();
        for(EventKey key : eventKeys)
        {
            keys.add(key.getKey());
        }

        return keys;
    }

    public Boolean getIsRequeue() {
        return isRequeue;
    }

    public void setIsRequeue(Boolean isRequeue) {
        this.isRequeue = isRequeue;
    }

    public KeysetId getKeysetId() {
        return keysetId;
    }

    public ETLTimestamp getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(ETLTimestamp actionTimestamp) {
        this.eventTimestamp = actionTimestamp;
    }

    public Ingestion.ReplicationType getUpdateType() {
        return updateType;
    }

    public void setUpdateType(Ingestion.ReplicationType updateType) {
        this.updateType = updateType;
    }

    public void setEventKeys(List<EventKey> eventKeys) {
        this.eventKeys = eventKeys;
    }

    private Boolean isValid()
    {
        if (updateType ==null || keysetId == null || eventTimestamp == null)
        {
            logger.atWarn().addJson("event", toJson()).log("Invalid Event found for keysetId '{}.{}'", keysetId);
            return false;
        }
        return true;
    }
}
