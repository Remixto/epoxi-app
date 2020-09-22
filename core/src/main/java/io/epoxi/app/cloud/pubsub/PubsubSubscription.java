package io.epoxi.app.cloud.pubsub;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.TopicName;

import org.slf4j.Marker;

import io.epoxi.app.cloud.logging.AppLog;
import io.epoxi.app.cloud.logging.StatusCode;

import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

public class PubsubSubscription {

    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

    private final Subscription baseSubscription;

    public PubsubSubscription(Subscription subscription)
    {
        Marker marker = logger.logEntry("Constructor");

        this.baseSubscription = subscription;

        logger.atDebug().log("PubsubSubscription object initialized - '{}'", getName());
        logger.logExit(marker);
    }

    /**
     * Examines the subscription for a label called 'numOfMessages' that indicates the number of
     * messages that should be received from the subscription when receiving messages.
     * If 'numOfMessages' is not found, the default number of messages to receive is 100.
     * @return An integer value
     */
    public Integer numOfMessages()
    {
        String label = getBaseSubscription().getLabelsMap().get("numOfMessages");
        return (label ==null) ? 100: Integer.parseInt(label);
    }

    public static Subscription createSubscription(TopicName topicName, ProjectSubscriptionName subscriptionName, Integer ackDeadlineSeconds) throws IOException {

        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {

          // create a pull subscription with default acknowledgement deadline
          final Subscription subscription = subscriptionAdminClient
              .createSubscription(
                    subscriptionName, topicName, PushConfig.getDefaultInstance(), ackDeadlineSeconds);

          logger.atInfo().log("PubSub subscription '{}' created in Cloud", subscriptionName.getSubscription());

          return subscription;
        }
    }

    public SubscriberStub getSubscriberStub()
    {
        SubscriberStub subscriber;

        try {
            // [START pubsub_subscriber_sync_pull]
            SubscriberStubSettings subscriberStubSettings =
            SubscriberStubSettings.newBuilder()
              .setTransportChannelProvider(
                  SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                      .setMaxInboundMessageSize(20 << 20) // 20MB
                      .build())
              .build();

            subscriber = GrpcSubscriberStub.create(subscriberStubSettings);
            logger.atDebug().log("Subscriber found for Subscription '{}'", getName());

            return subscriber;
        }
        catch (IOException ex)
        {
            String msg = String.format("Error obtaining a subscriber for the Subscription '%s'", getName());
            throw new PubsubException(msg, ex, StatusCode.UNAVAILABLE);
        }
    }

    public List<ReceivedMessage> pullMessages(SubscriberStub subscriber, Integer numOfMessages)
    {
        try {

            PullRequest pullRequest =
                PullRequest.newBuilder()
                    .setMaxMessages(numOfMessages)
                    .setSubscription(getName())
                    .build();

            // use pullCallable().futureCall to asynchronously perform this operation
            PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);

            List<ReceivedMessage> messages = pullResponse.getReceivedMessagesList();
            logger.atDebug().log("{} message(s) pulled from Subscription '{}'", messages.size(), getName());

            return messages;
        }
        catch (ApiException ex)
        {
            String msg = String.format("Error pulling messages from Subscription '%s'", getName());

            throw new PubsubException(msg, ex, StatusCode.of(ex.getStatusCode()));
        }
    }

    public void clearMessages()
    {
        clearMessages(10000);
    }

    public void clearMessages(Integer numToClear)
    {

        try (SubscriberStub subscriber = getSubscriberStub()) {
            List<ReceivedMessage> messages = pullMessages(subscriber, numToClear);
            List<AckReply> acks = new ArrayList<>();
            for (ReceivedMessage message : messages) {
                AckReply reply = new AckReply(message);
                reply.ack();
                acks.add(reply);
            }

            acknowledgeMessages(subscriber, acks, false);
            logger.atInfo().log("{} message(s) cleared from subscription '{}'", acks.size(), getName());
        } catch (PubsubException ex) {
            logger.atError().log("Failed to clear messages from subscription '{}'", getName());
        }
    }

    public void acknowledgeMessages(SubscriberStub subscriber, List<AckReply> acks, Boolean ackOnlySuccess)
    {
        if (acks.isEmpty()) return;

        List<String> acksToAcknowledge = getAcks(acks, ackOnlySuccess);

         // acknowledge received messages
         if (!acksToAcknowledge.isEmpty())
         {
             AcknowledgeRequest acknowledgeRequest =
             AcknowledgeRequest.newBuilder()
                 .setSubscription(getName())
                 .addAllAckIds(acksToAcknowledge)
                 .build();
             // use acknowledgeCallable().futureCall to asynchronously perform this operation
             subscriber.acknowledgeCallable().call(acknowledgeRequest);

             logger.atInfo().log("{} message(s) acknowledged for Subscription '{}'", acks.size(), getName());

             if (acksToAcknowledge.size() != acks.size())
             {
                Integer numNotAcknowledged = acks.size() - acksToAcknowledge.size();
                logger.atWarn().log("{} message(s) not acknowledged for Subscription '{}'", numNotAcknowledged, getName());
             }
         }
    }

    private List<String> getAcks(List<AckReply> ackIds, Boolean ackOnlySuccess)
    {
        List<String> acks = new ArrayList<>();

        for (AckReply reply : ackIds) {
            if (Boolean.TRUE.equals(!ackOnlySuccess) || Boolean.TRUE.equals(reply.getAckReply()))
            {
                acks.add(reply.getAckId());
            }
        }

        if (acks.isEmpty())
        {
            logger.atWarn().addKeyValue("ackOnlySuccess", ackOnlySuccess).log("0 ack(s) identified. It is likely that something did not complete correctly");
        }
        else
        {
            logger.atDebug().addKeyValue("ackOnlySuccess", ackOnlySuccess).log("'{}' ack(s) identified", acks.size());
        }

        return acks;
    }


    /**
    * Gets the topic that is associated with the specified subscription
    */
    public PubsubTopic getTopic()
    {
        TopicName topicName = PubsubTopic.toTopicName(baseSubscription.getTopic());
        PubsubTopic topic = PubsubTopic.of(topicName);

        logger.atDebug().addKeyValue("topicId", topicName.getTopic()).log("PubsubTopic found for subscription '{}'", this.getName());

        return topic;
    }

    public Boolean isPushSubscription()
    {
        return !(baseSubscription.getPushConfig().getPushEndpoint().equals(""));
    }

    public Boolean isPullSubscription()
    {
        return !isPushSubscription();
    }

    public Subscription getBaseSubscription()
    {
        return baseSubscription;
    }

    public String getName()
    {
        return baseSubscription.getName();
    }

    public static PubsubSubscription of(String projectId, String subscriptionId)
    {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
        return of(subscriptionName);
    }

    /**
     * Gets a subscription from PubSub.  If the subscriptionName is not found, a null value will be returned.
     * @param subscriptionName The full name of the subscription in GCP Pubsub
     * @return A PubsubSubscription object
     */
    public static PubsubSubscription of(ProjectSubscriptionName subscriptionName)
    {
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
            Subscription subscription = subscriptionAdminClient.getSubscription(subscriptionName);
            return new PubsubSubscription(subscription);
        }
        catch (NotFoundException ex) {
            logger.atError().log("Subscription '{}' not found", subscriptionName);
        }
        catch (IOException ex) {
            String msg = String.format("Cannot connect to PubsubSubscription to retrieve subscription '%s'", subscriptionName);
            throw new PubsubException(msg, ex, StatusCode.INTERNAL);
        }

        return null;

    }

}

