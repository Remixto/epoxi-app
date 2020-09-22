package io.epoxi.app.cloud.logging;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AppLog
{
    final Logger logger;

    public AppLog (Class<?> object)
    {
        this.logger = LoggerFactory.getLogger(object);
    }

    public AppLog.Builder atError()
    {
        return new AppLog.Builder(Type.ERROR);
    }

    public AppLog.Builder atInfo()
    {
        return new AppLog.Builder(Type.INFO);
    }

    public AppLog.Builder atWarn()
    {
        return new AppLog.Builder(Type.WARN);
    }

    public AppLog.Builder atEntry()
    {
        return new AppLog.Builder(Type.TRACE).addMessage("Entering");
    }

    public AppLog.Builder atExit()
    {
        return new AppLog.Builder(Type.TRACE).addMessage("Exiting");
    }

    public AppLog.Builder atDebug()
    {
        return new AppLog.Builder(Type.DEBUG);
    }

    public Marker logEntry(String markerName)
    {
        Marker marker = MarkerFactory.getMarker(markerName);

        atEntry()
            .addMarker(marker)
            .log();

        return marker;
    }

    public void logExit(String markerName)
    {
        Marker marker = MarkerFactory.getMarker(markerName);
        logExit(marker);
    }

    public void logExit(Marker marker)
    {
        atExit()
            .addMarker(marker)
            .log();
    }

    public static Builder newBuilder(String json) {
        GsonBuilder builder = new GsonBuilder();
        Gson gson = builder.create();
        return gson.fromJson(json, Builder.class);
    }

    public class Builder
    {
        String messageTemplate;
        @Getter final List<Object> args = new ArrayList<>();

        @Expose String message ;
        @Expose @Getter final Map<String, String> labels = new HashMap<>();
        @Expose @Getter final Map<String, String> jsons = new HashMap<>();

        AppLog.Type type;

        protected Builder (AppLog.Type type)
        {
            this.type = type;
        }

        /**
         * Add a message to be sent to the log.
         * Repeated calls to this method will overwrite the message
         * @param message The message that will be logged
         * @return The fluid builder for AppLog
         */
        private AppLog.Builder addMessage(String message)
        {
            this.messageTemplate = message;
            return this;
        }

        public AppLog.Builder addMessage(String message, Object arg1)
        {
            this.messageTemplate = message;
            args.clear();
            addArg(arg1);
            return this;
        }

        public AppLog.Builder addMessage(String message, Object arg1, Object arg2)
        {
            this.messageTemplate = message;
            args.clear();
            addArgs(arg1, arg2);
            return this;
        }

        public AppLog.Builder addMessage(String message, List<Object> argList)
        {
            this.messageTemplate  = message;
            args.clear();
            addArgs(argList);
            return this;
        }

        public AppLog.Builder addArg(Object arg)
        {
            args.add(arg);
            return this;
        }

        public AppLog.Builder addArgs(Object arg1, Object arg2)
        {
            args.add(arg1);
            args.add(arg2);
            return this;
        }

        public AppLog.Builder addArgs(List<Object> argList)
        {
            args.addAll(argList);
            return this;
        }
        public AppLog.Builder addMarker(Marker marker)
        {
            addKeyValue("Marker", marker.toString());
            return this;
        }

        public AppLog.Builder addKeyValue(String key, Object value)
        {
            labels.put(key, value.toString());
            return this;
        }

        public AppLog.Builder addJson(String key, String json)
        {
            jsons.put(key, json);
            return this;
        }

        public AppLog.Builder addException(Throwable exception)
        {
            addKeyValue("Inner Exception", exception.getMessage());
            return this;
        }

        public void log(String message)
        {
            addMessage(message);
            log();
        }

        public void log(String message, Object arg1)
        {
            addMessage(message, arg1);
            log();

        }

        public void log(String message, Object arg1, Object arg2)
        {
            addMessage(message, arg1, arg2);
            log();
        }

        public void log(String message, List<Object> args)
        {
            addMessage(message, args);
            log();
        }

        private void log()
        {
            switch(type)
            {
                case INFO:
                    if (logger.isInfoEnabled()) logger.info(getTextPayload());
                    break;
                case WARN:
                    if (logger.isWarnEnabled()) logger.warn(getTextPayload());
                    break;
                case ERROR:
                    if (logger.isErrorEnabled()) logger.error(getTextPayload());
                    break;
                case DEBUG:
                    if (logger.isDebugEnabled()) logger.debug(getTextPayload());
                    break;
                case TRACE:
                    if (logger.isTraceEnabled()) logger.trace(getTextPayload());
                    break;
            }
        }

        private String getTextPayload()
        {
            GsonBuilder builder = new GsonBuilder();
            builder.excludeFieldsWithoutExposeAnnotation();
            Gson gson = builder.create();

            //Build a json object that contains the message, the labels, and the json objects
            String msg = messageTemplate.replace("{}", "%s");
            message = String.format(msg, args.toArray());
            return gson.toJson(this);
        }
    }

    private enum Type {INFO, WARN, ERROR, DEBUG, TRACE}

}
