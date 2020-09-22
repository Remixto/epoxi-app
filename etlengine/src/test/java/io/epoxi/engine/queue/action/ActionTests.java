package io.epoxi.engine.queue.action;

import com.google.pubsub.v1.PubsubMessage;
import io.epoxi.engine.EngineTestDataFactory;
import io.epoxi.repository.TestConfig;
import io.epoxi.repository.model.StepType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ActionTests {

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
    public void createActionTest() {

        Action action = factory.getTestAction(StepType.EXTRACT);
        assertNotNull(action, "Action successfully created");
    }

    @Test
    public void createActionFromMessageTest() {

        PubsubMessage message = factory.getTestEvent().toMessage();
        Action action = Action.fromMessage(StepType.EXTRACT, message);

        assertNotNull(action, "Action successfully created from PubsubMessage");
    }

    private static void clean() {

    }

    private static void init() {

    }




}
