package io.epoxi.etlengine.api;

import com.google.pubsub.v1.PubsubMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class PushSubscriptionTaskTest {

    @BeforeAll
    public static void setup() {
        init();
        clean();
    }

    @AfterAll
    public static void tearDown() {

    }

    @Test @Disabled
    public void getPushSubscriptionTaskExtractETLStepTest() {

        PubsubMessage message = null;
        String name = "source";

        EtlEngineApiController engineController = new EtlEngineApiController();
        String msgString = message.getData().toStringUtf8();
        assertDoesNotThrow(() -> engineController.receiveMessage(name, msgString, false), "Successfully processed message received from (Push) Pubsub Subscription");
    }

    private static void init() {
        EtlEngineApiTestDataFactory.init();
    }
    private static void clean() {}
}
