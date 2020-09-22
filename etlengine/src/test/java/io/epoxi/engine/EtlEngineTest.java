package io.epoxi.engine;

import io.epoxi.cloud.logging.AppLog;
import io.epoxi.engine.queue.EngineSubscription;
import io.epoxi.engine.queue.PullSubscriptionTask;
import io.epoxi.engine.etl.EtlAllPipelinesTest;
import io.epoxi.repository.TestConfig;
import io.epoxi.repository.model.MessageQueue;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandles;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class EtlEngineTest {

    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());
    private static final Integer pullDelay = -1;
    private static final Boolean isRunAsync = false;
    private static List<MessageQueue> messageQueues;


    @BeforeAll
    public static void setup() {
        clean();
        init();
    }

    @AfterAll
    public static void tearDown() {

    }

    @Test
    public static void integrationTest() {

        assertDoesNotThrow(EtlEngineTest::readPullSubscriptions, "EtlEngine Integration Test Successful");
    }

    private static void clean() {

        //Clear the CDC queue
        EtlAllPipelinesTest.setup();
    }

    private static void init() {
        EngineTestDataFactory.init();
        //Populate the list of message queues
        messageQueues = new EngineTestDataFactory(TestConfig.engineTestAccountName).getMessageQueues();
    }

    private static void readPullSubscriptions()
    {
        //Copy a list of the current message queues and sort it by weight
        //We will iterate a copy, because the core list could be added to during our iteration
        List<MessageQueue> queuesToPull = new ArrayList<>(messageQueues);
        queuesToPull.sort(new MessageQueue.SortByWeight());

        for (MessageQueue messageQueue : messageQueues)
        {
            logger.atDebug().log("Reading Pull Subscription '{}''", messageQueue.getName());

            EngineSubscription engineSubscription = EngineSubscription.of(messageQueue);
            //Process only subscriptions that are pull based
            if (Boolean.TRUE.equals(engineSubscription.isPullSubscription()))
            {
                TimerTask task = PullSubscriptionTask.of(engineSubscription);
                if (Boolean.FALSE.equals(isRunAsync))
                {
                    task.run();
                }
                else
                {
                    new Timer().schedule(task, 0);
                }
            }
        }

        if (pullDelay == -1)
        {
            return;
        }

        long delay = pullDelay * 1000L;

        try
        {
            Thread.sleep(delay);
        }
        catch (InterruptedException ex)
        {
            //Log and return nothing
            Thread.currentThread().interrupt();
            logger.atWarn().log("Controller interrupted");
        }

        //Continue to run this method until it is stopped.
        readPullSubscriptions();
    }
}
