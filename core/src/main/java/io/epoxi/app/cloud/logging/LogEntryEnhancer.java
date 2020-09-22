package io.epoxi.app.cloud.logging;

import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.LoggingEnhancer;
import com.google.cloud.logging.Payload.JsonPayload;
import com.google.cloud.logging.Payload.StringPayload;

import java.util.HashMap;
import java.util.Map;

// Add / update additional fields to the log entry
public class LogEntryEnhancer implements LoggingEnhancer {

  @Override
  public void enhanceLogEntry(LogEntry.Builder logEntry) {

    JsonPayload payload = logEntry.build().getPayload();
    String textPayload = payload.getDataAsMap().get("message").toString();

    //Get the appLogEntry
    AppLog.Builder appLogEntry = AppLog.newBuilder(textPayload);

    //Transfer data from appLogEntry to logEntry (stackDriver logEntry)
    Map<String, String> allLabels = new HashMap<>();
    allLabels.putAll(appLogEntry.getLabels());
    allLabels.putAll(appLogEntry.getJsons());

    logEntry.setLabels(allLabels);
    logEntry.setPayload(StringPayload.of(appLogEntry.message));

  }
}
