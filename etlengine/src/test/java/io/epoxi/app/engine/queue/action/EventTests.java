package io.epoxi.app.engine.queue.action;

import com.google.pubsub.v1.PubsubMessage;
import io.epoxi.app.engine.EngineTestDataFactory;
import io.epoxi.app.repository.TestConfig;
import io.epoxi.app.repository.event.Event;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class EventTests {

    EngineTestDataFactory factory = new EngineTestDataFactory(TestConfig.engineTestAccountName);

    @BeforeAll
    public static void setup() {
        clean();
        init();
    }

    @AfterAll
    public static void tearDown() {

    }

    @Test
    public void createEventFromKeysetIdTest() {

        Event event = factory.getTestEvent();
        assertNotNull(event, "Event successfully created");
    }

    @Test
    public void createEventFromMessageTest() {

        PubsubMessage message = factory.getTestEvent().toMessage();
        Event event = Event.fromMessage(message);

        assertNotNull(event, "Event successfully created from PubsubMessage");
    }

    @Test
    public void eventToMessageTest() {

        Event event = factory.getTestEvent();
        PubsubMessage message = event.toMessage();

        assertNotNull(message, "Event successfully converted to PubsubMessage");

    }


    private static void clean() {


    }

    private static void init() {

    }



}
