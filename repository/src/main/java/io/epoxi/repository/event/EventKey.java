package io.epoxi.repository.event;

import com.google.gson.GsonBuilder;
import com.google.pubsub.v1.PubsubMessage;

import lombok.Getter;
import lombok.Setter;

import com.google.gson.FieldNamingStrategy;
import com.google.gson.Gson;
import java.lang.reflect.Field;

public class EventKey {
    
    @Getter @Setter
    private Long key;

    @Getter @Setter
    private Type event;
  
    public EventKey()
    {}

    public EventKey(Long key, Type event)
    {
        this.key = key;
        this.event = event;
    }

    public static EventKey fromMessage(PubsubMessage message)
    {
        String json = message.getData().toStringUtf8();
        return fromJson(json, EventKey.class);
    }

    public static EventKey fromJson(String json, Class<EventKey> object)
    {
        GsonBuilder builder = new GsonBuilder();
        Gson gson = builder.create();

        return gson.fromJson(json, object);     
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

    public static Type toEventEnum(String code) {
        
        switch (code)
        {
            case "I":
                return Type.INSERT;
            case "U":
                return Type.UPDATE;
            case "D":
                return Type.DELETE;
            default:
                return null;
        }
    }

    public enum Type {
        INSERT,
        UPDATE,
        DELETE,
        REQUEUE
    }
    
 }


 