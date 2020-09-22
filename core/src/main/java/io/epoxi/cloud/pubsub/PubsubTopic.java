package io.epoxi.cloud.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.ProjectSubscriptionName;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;

import io.epoxi.cloud.logging.StatusCode;
import org.slf4j.Marker;

import lombok.Getter;
import io.epoxi.cloud.logging.AppLog;

import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

/**
 * A wrapper for a Google Cloud Pub/Sub Topic The constructor will throw an IO
 * exception if the TopicAdminClient is unavailable, or an ApiException if the
 * Topic does not exist All topics are created via the static methods.
 */
public class PubsubTopic {

  private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

  @Getter
  final Topic topic;

  private PubsubTopic(final Topic topic) {
    this.topic = topic;
  }

  public PubsubWriter getWriter() {
    return PubsubWriter.create(this);
  }

  public static Topic createTopic(TopicName topicName) throws IOException {

    Topic topic;

    // Create the topic
    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      topic = topicAdminClient.createTopic(topicName);
      logger.atInfo().log("PubSub topic '{}' created in Cloud", topicName.getTopic());
    }

    return topic;

  }

  protected void publishMessage(final PubsubMessage message) throws InterruptedException{

    final PubsubMessage[] messages = { message };
    publishMessages(messages);
  }

  protected void publishMessages(final PubsubMessage[] messages) throws InterruptedException{
    // [START pubsub_publish_error_handler]

    Publisher publisher = null;

    try {
      // Create a publisher instance with default settings bound to the topic
      publisher = Publisher.newBuilder(getTopicName()).build();

      logger.atDebug().log("Writing {} messages to Topic '{}'", messages.length, topic.getName());

      for (final PubsubMessage message : messages) {

        // Once published, returns a server-assigned message id (unique within the
        // topic)
        final ApiFuture<String> future = publisher.publish(message);

        // Add an asynchronous callback to handle success / failure
        ApiFutures.addCallback(future, new ApiFutureCallback<>() {

          @Override
          public void onFailure(final Throwable throwable) {
            if (throwable instanceof ApiException) {
              throw  ((ApiException) throwable);
            }
          }

          @Override
          public void onSuccess(final String messageId) {
            // Once published, returns server-assigned message ids (unique within the topic)
            logger.atDebug().addKeyValue("messageId", messageId).log("Message has been written to topic '{}'",
                topic.getName());
            }
          }, MoreExecutors.directExecutor());

        }
      }
      catch(IOException ex)
      {
        throw new PubsubException(CONNECTION_ERROR, ex, StatusCode.UNAVAILABLE);
      }

     finally {
      if (publisher != null) {
        // When finished with the publisher, shutdown to free up resources.
        publisher.shutdown();
        publisher.awaitTermination(1, TimeUnit.MINUTES);
      }
    }
    // [END pubsub_publish_error_handler]
  }

  public static ProjectSubscriptionName getDefaultSubscriptionName(TopicName topicName) {
    String subscriptionId = topicName.getTopic() + "_reader";
    final ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(topicName.getProject(), subscriptionId);

    logger.atDebug().addKeyValue("topicName", topicName.getTopic()).log("Default Subscription '{}' was identified",
        subscriptionName.getSubscription());

    return subscriptionName;

  }

  public TopicName getTopicName() {
    return toTopicName(topic.getName());
  }


  /**
   * Gets a TopicName object from a string
   *
   * @param topicName Must be passed in the form
   *                  Projects/{ProjectID}/Topics/{TopicId}
   */
  public static TopicName toTopicName(String topicName) {
    String[] parts = topicName.split("/");
    return TopicName.of(parts[1], parts[3]);
  }

  public static Boolean exists(TopicName topicName)
  {
    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      for(Topic topic : topicAdminClient.listTopics(topicName.getProject()).iterateAll())
      {
        if(topic.getName().equals(topicName.getTopic()))
        {
          return true;
        }
      }

      return false;
    }
    catch(IOException ex)
    {
        throw new PubsubException(CONNECTION_ERROR, ex, StatusCode.UNAVAILABLE);
    }
    catch(ApiException ex)
    {
      String msg = String.format("Error while checking existence of Pubsub topic '%s.%s'", topicName.getProject(), topicName.getTopic());
      throw new PubsubException(msg,  ex, StatusCode.of(ex.getStatusCode()));
    }
  }

  /**
   * Returns a topic based on a fully qualified TopicName string If the topic is not found, a null value will be returned
   */

  public static PubsubTopic of(String topicName) {
    return of(toTopicName(topicName));
  }

  public static PubsubTopic of(String projectId, String topicId) {
    return of(TopicName.of(projectId, topicId));
  }

  /**
   * Returns a topic based on a TopicName. If the topic is not found, a null value will be returned
   */
  public static PubsubTopic of(TopicName topicName) {

    if (topicName ==null) return null;

    // Attempt to retrieve the topic
    Marker marker = logger.logEntry("Constructor");

    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      Topic topic = topicAdminClient.getTopic(topicName);
      logger.atDebug().log("PubsubTopic object initialized - '{}''", topicName);
      return of(topic);
    }
    catch(NotFoundException ex)
    {
      logger.atError().log("Topic '{}' not found", topicName);
    }
    catch(IOException ex)
    {
        throw new PubsubException(CONNECTION_ERROR, ex, StatusCode.UNAVAILABLE );
    }
    catch(ApiException ex)
    {
      throw new PubsubException("Cannot get topic for the subscription", ex, StatusCode.of(ex.getStatusCode()));
    }

    logger.logExit(marker);
    return null;
  }

  public static PubsubTopic of(Topic topic) {
    return new PubsubTopic(topic);
  }

  private static final String CONNECTION_ERROR = "Cannot connect to Pubsub";

}
