package io.epoxi.cloud.logging;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.lang.invoke.MethodHandles;

import org.junit.jupiter.api.Test;

public class LogEntryEnhancerTest {

    @Test
    public void logWithKeyValueTest()
    {
        AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());
        assertDoesNotThrow(() -> logger.atInfo().addKeyValue("Key", "value").log("Test Info with key/value"),  "Info (with a key/value pair) was successfully logged");
    }

    @Test
    public void logWithJsonTest()
    {
        AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());
        String json = "{\"GlossSee\": \"markup\"}";

        assertDoesNotThrow(() -> logger.atInfo().addJson("Json Test", json).log("Test Info with Json"),  "Info (with json) was successfully logged");
    }

}
