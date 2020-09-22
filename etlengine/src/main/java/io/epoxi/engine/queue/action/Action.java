package io.epoxi.engine.queue.action;

import io.epoxi.repository.event.Event;
import io.epoxi.repository.model.KeysetId;
import io.epoxi.repository.model.StepType;
import io.epoxi.repository.model.Ingestion.ReplicationType;
import io.epoxi.repository.event.ETLTimestamp;
import io.epoxi.cloud.logging.AppLog;

import org.slf4j.Marker;

import com.google.gson.FieldNamingStrategy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.pubsub.v1.PubsubMessage;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;

public class Action {

    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

    private final Event event;
    private final StepType stepType;

    public Action(StepType stepType, Event event)
    {
        this.stepType = stepType;
        this.event = event;

        logger.atDebug().addJson("action", toJson()).log("ETLAction created");
    }

    public static Action fromJson(String json)
    {
        Marker marker = logger.logEntry("Constructor");

        GsonBuilder builder = new GsonBuilder();
        Gson gson = builder.create();
        Action action = gson.fromJson(json, Action.class);

        if (Boolean.FALSE.equals(action.isValid()))
        {
            //Warning
            logger.atWarn().addJson("constructor-json", json).log("Cannot create ETLAction from json.");
            action = null;
        }
        else
        {
            logger.atDebug().addJson("constructor-json", json).log("ETLAction created from Json with an event containing {} keys", action.event.getEventKeys().size());
        }

        logger.logExit(marker);
        return action;
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

    private Boolean isValid()
    {
        if (event ==null || stepType == null )
        {
            logger.atDebug().addJson("action", toJson()).log("Invalid Action found");
            return false;
        }
        return true;
    }

    public StepType getStepType() {
        return stepType;
    }

    public Event getEvent() {
        return event;
    }

    public Boolean getIsRequeue() {
        return event.getIsRequeue();
    }

    public KeysetId getKeysetId() {
        return event.getKeysetId();
    }

    public ETLTimestamp getActionTimestamp() {
        return event.getEventTimestamp();
    }

    public ReplicationType getUpdateType() {
        return event.getUpdateType();
    }

    public static Action fromMessage(StepType type, PubsubMessage message)
    {
        Action action = null;
        Event event = Event.fromMessage(message);
        if (event!=null) action = new Action(type, event);

        return action;

    }

}
