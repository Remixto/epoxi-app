package io.epoxi.app.cloud.logging;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import java.lang.invoke.MethodHandles;

import org.junit.jupiter.api.Test;


public class AppLogTest
{
    @Test
    public void logErrorTest()
    {
        AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

        assertDoesNotThrow(() -> logger.atError().log("Test Error"),  "An error was successfully logged");
    }

}
