package io.epoxi.cloud.pubsub;

import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.TopicName;
import io.epoxi.cloud.TestConfig;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class PubsubSubscriptionTest {

    @Test
    public void createPubsubSubscriptionFromName()
    {
        //Test a non existent subscription
        PubsubSubscription subscriptionNotExists = PubsubSubscription.of(TestConfig.PUBSUB_PROJECT_ID, "test");
        assertNull(subscriptionNotExists, "PubsubSubscription successfully created from name");

        //Test subscription that exists
        PubsubSubscription subscriptionExists = PubsubSubscription.of(TestConfig.PUBSUB_PROJECT_ID, "test_reader");
        assertNotNull(subscriptionExists, "PubsubSubscription successfully created from name");
    }

    @Test
    public void publishMessagesTest()  {
        PubsubMessage message = PubsubMessage.getDefaultInstance();
        TopicName topicName = TopicName.of(TestConfig.PUBSUB_PROJECT_ID, "test");
        PubsubTopic topic = PubsubTopic.of(topicName);
        assertDoesNotThrow(()->topic.publishMessage(message));
    }

    @Test
    public void pullMessagesTest()
    {
        publishMessagesTest();

        PubsubSubscription subscription = PubsubSubscription.of(TestConfig.PUBSUB_PROJECT_ID, "test_reader");
        SubscriberStub subscriber = subscription.getSubscriberStub();
        Integer numMessagesToPull = 100;

        List<ReceivedMessage> messages = subscription.pullMessages(subscriber, numMessagesToPull);
        assertFalse(messages.isEmpty(), "Messages successfully received");
    }

    @Test
    void createSubscription() {
        //TODO
    }
}
