package io.epoxi.app.engine.queue;

import io.epoxi.app.engine.EngineTestDataFactory;
import io.epoxi.app.repository.model.StepType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class EngineSubscriptionTest {

    @BeforeAll
    public static void setup() {
        init();
        clean();
    }

    @AfterAll
    public static void tearDown() {

    }

    @Test
    public void getStepTypeTest() {

        String name;

        name = "source_reader";
        EngineSubscription engineSubscriptionSource = EngineSubscription.of(name);
        Assertions.assertEquals(StepType.EXTRACT, engineSubscriptionSource.getStepType(), "Successfully obtained Extract StepType from (Pull) Pubsub Subscription");

        name = "etl_transform_reader";
        EngineSubscription engineSubscriptionTransform = EngineSubscription.of(name);
        Assertions.assertEquals(StepType.LOAD, engineSubscriptionTransform.getStepType(), "Successfully obtained Load StepType from (Pull) Pubsub Subscription");

        name = "etl_load_reader";
        EngineSubscription engineSubscriptionLoad = EngineSubscription.of(name);
        Assertions.assertNull(engineSubscriptionLoad.getStepType(), "Successfully obtained Load StepType from (Pull) Pubsub Subscription");
    }

    private static void init() {
        EngineTestDataFactory.init();
    }

    private static void clean() {

    }

}
