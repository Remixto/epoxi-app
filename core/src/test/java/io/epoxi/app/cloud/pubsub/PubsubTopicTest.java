package io.epoxi.app.cloud.pubsub;

import io.epoxi.app.cloud.TestConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class PubsubTopicTest {

    @Test
    public void createPubsubTopicFromNameTest()
    {
       //Test a non existent subscription
       PubsubTopic topic = PubsubTopic.of(TestConfig.PUBSUB_PROJECT_ID, "testNotExists");
       assertNull(topic, "PubsubTopic successfully NOT obtained from a non existent name");
    }

    @Test
    public void getPubsubTopicFromNameTest()
    {
        //Test subscription that exists
        PubsubTopic topic = PubsubTopic.of(TestConfig.PUBSUB_PROJECT_ID, "test");
        assertNotNull(topic, "PubsubTopic successfully obtained from name");
    }
}
