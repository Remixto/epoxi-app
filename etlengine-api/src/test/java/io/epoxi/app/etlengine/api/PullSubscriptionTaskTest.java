package io.epoxi.app.etlengine.api;

import io.epoxi.app.repository.TestConfig;
import io.epoxi.app.repository.model.Ingestion;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class PullSubscriptionTaskTest {

    @BeforeAll
    public static void setup() {
        init();
        clean();
    }

    @AfterAll
    public static void tearDown() {

    }

    @Test
    public void getPullSubscriptionTaskExtractETLStepTest() {

        //Place a message on the queue
        EtlEngineApiTestDataFactory apiFactory = new EtlEngineApiTestDataFactory(TestConfig.engineTestAccountName);
        Ingestion ingestion = apiFactory.getTestIngestion("Firm_Data_Entity", false);

        apiFactory.getAccountRepository().getIngestionSyncRepository().add(ingestion);

        //Receive the message
        String subscriptionName = "source_reader";
        EtlEngineApiController engineController = new EtlEngineApiController();
        assertDoesNotThrow(() -> engineController.receiveMessages(subscriptionName, false), "Successfully processed message received from (Push) Pubsub Subscription");
    }

    private static void init() {
        EtlEngineApiTestDataFactory.init();
    }

    private static void clean() {

    }

}
