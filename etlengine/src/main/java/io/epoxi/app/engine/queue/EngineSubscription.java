package io.epoxi.app.engine.queue;

import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.Subscription;
import io.epoxi.app.engine.queue.action.Action;
import io.epoxi.app.engine.queue.action.ActionReceiver;
import io.epoxi.app.cloud.logging.AppLog;
import io.epoxi.app.cloud.logging.StatusCode;
import io.epoxi.app.cloud.pubsub.PubsubException;
import io.epoxi.app.cloud.pubsub.PubsubSubscription;
import io.epoxi.app.cloud.pubsub.PubsubTopic;
import io.epoxi.app.engine.Config;
import io.epoxi.app.engine.EngineException;
import io.epoxi.app.engine.etl.ETLStep;
import io.epoxi.app.engine.etl.pipeline.PipelineException;
import io.epoxi.app.repository.AccountRepository;
import io.epoxi.app.repository.IngestionRepository;
import io.epoxi.app.repository.MessageQueueRepository;
import io.epoxi.app.repository.exception.NotFoundException;
import io.epoxi.app.repository.model.Ingestion;
import io.epoxi.app.repository.model.KeysetId;
import io.epoxi.app.repository.model.MessageQueue;
import io.epoxi.app.repository.model.MessageQueue.QueueType;
import io.epoxi.app.repository.model.StepType;
import lombok.NonNull;

import java.lang.invoke.MethodHandles;
import java.util.List;

/**
 * Returns metadata about the subscription based on its attributes (such as name)
 * and how these attributes are interpreted by the business logic of the ETLEngine
 */
public class EngineSubscription extends PubsubSubscription{
    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

    public EngineSubscription (PubsubSubscription subscription)
    {
      super(subscription.getBaseSubscription());
    }

    public EngineSubscription (Subscription subscription)
    {
      super(subscription);
    }

    /**
     * Get a subscriber for the subscription that will pull messages
     * @return a reader for the Pull Subscription
     */
    public PullSubscriptionReader getPullSubscriberReader()
    {
        return  new PullSubscriptionReader(this);
    }

    public ActionReceiver getMessageReceiver(Action action)
    {
         //If the subscription is linked to a keyset, get the receiver that will be used for all buffering.
        //If not, the currentReceiver will be null and set later, based on the message
        ActionReceiver receiver = getMessageReceiver();

        if (receiver == null)
        {
            receiver = getMessageReceiver(action.getKeysetId());
        }

        return receiver;
    }


    /**
     * If the subscription contains a keysetId, it is possible to get a receiver for it.
     * If null is returned, then this is not possible
     */
    private ActionReceiver getMessageReceiver()
    {
        ActionReceiver receiver;

        KeysetId keysetId = getKeysetId();
        if(keysetId != null)
        {
            receiver = getMessageReceiver(keysetId);
            if (receiver != null)
            {
                logger.atDebug().log("Receiver '{}' assigned to the Subscription '{}'", receiver.getName(), getName());
                return receiver;
            }
            else
            {
                logger.atDebug().log("A receiver based on the keysetId '{}' could not be identified for the Subscription '{}'", keysetId, getName());
            }
        }

        return null;
    }

      /**
     * Builds an ActionReceiver (ETLStep) based on data found within the subscription.
     * If keysetId is found within the subscription, the pipeline for the ETLStep will be configured.
     * @param keysetId to be checked for keysetId labels
     * @return An ETLStep
     */
    private ActionReceiver getMessageReceiver(KeysetId keysetId)
    {
        String keyValue = "keysetId";

        try {
            Ingestion ingestion = IngestionRepository.getIngestion(keysetId);

            //An ingestion was found.  Return the step that matches the stepType for the ingestion
            if(ingestion != null)
            {
                ETLStep etlStep = new ETLStep(ingestion, stepType);
                logger.atDebug().log("Receiver '{}' created for type '{}'", etlStep.getName(), stepType);

                return etlStep;
            }
            else
            {
                logger.atDebug().log("No Receiver found for the type '{}' based on keyset '{}'", stepType, keysetId);
            }

        }
        catch(NotFoundException ex )
        {
            //A valid ingestion was not found.  Warn and exit
            logger.atWarn().addKeyValue(keyValue, keysetId).log("No ingestion was found that matches the KeysetId passed");
        }
        catch(PipelineException ex)
        {
            String msg = String.format("Error during creation of ETLStep with keysetId '%s''.  Pipeline could not be created", keysetId);
            throw new EngineException(msg, ex, StatusCode.INTERNAL);
        }

        return null;

    }

    /**
     * Check the topic of the subscription for a keysetId.  the keysetId is read as the 3rd dot eg. etl_extract.cdc.Firm_Data_Entity
     * @return A string that represents the KeySetId
     */
    public KeysetId getKeysetId() {

        try
        {
            KeysetId keysetId = getTopicKeysetId(getTopic());

            if (keysetId != null) logger.atDebug().addKeyValue("keysetId", keysetId).log("Keyset '{}' identified for the subscription '{}''", keysetId, getName());
            return keysetId;
        }
        catch (PubsubException ex)
        {
            logger.atError().log("Exception encountered obtaining keysetId for PubSub subscription '{}'", getName());
            return null;
        }
    }

    /**
     * The keysetId is read as the 3rd dot eg. For the topic named 'etl_extract.cdc.Entity', keysetId = 'Entity'
     * @return A keysetId
     */
    public KeysetId getTopicKeysetId(PubsubTopic topic)
    {
        String topicId = topic.getTopicName().getTopic();

        String[] parts = topicId.split("\\.");

        KeysetId keysetId = null;
        if (parts.length >= 4) keysetId = KeysetId.of(parts[2], parts[3]);

        if (keysetId != null)
        {
        logger.atDebug().addKeyValue("topicName", topicId).log("KeysetId '{}' was identified for the topic", keysetId);
        }

        return keysetId;

    }

    public StepType getStepType()
    {
        //If we have previously calculated this, return it
        if (stepType !=null) return stepType;

        //If the subscription is keyset based, revert back the the base, for the purposes of comparison
        String nameToCompare = getName();
        KeysetId keysetId = getKeysetId();
        if (keysetId!=null)
        {
            nameToCompare = nameToCompare.replace(keysetId.toString(), "{keysetId}");
        }

        MessageQueueRepository messageQueueRepository = new AccountRepository().getMessageQueueRepository();
        List<MessageQueue> queues = messageQueueRepository.search(QueueType.IN);

        for (MessageQueue messageQueue : queues)
        {
            if (messageQueue.getQueue().getPath().equals(nameToCompare))
            {
                //Build the step
                stepType = messageQueue.getStepType();
                break;
            }
        }

        if (stepType == null)
        {
            logger.atDebug().log("A StepType could not be determined for subscription '{}'", getName());
        }
        return stepType;
    }

    private StepType stepType;

	public static EngineSubscription of(@NonNull String subscriptionName) {
        PubsubSubscription subscription = PubsubSubscription.of(Config.PUBSUB_PROJECT_ID, subscriptionName);
        if (subscription==null) throw new IllegalArgumentException(String.format("Subscription name '%s' not found", subscriptionName));

		return new EngineSubscription(subscription);
	}

	public static EngineSubscription of(MessageQueue messageQueue) {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.parse(messageQueue.getQueue().getPath());
        PubsubSubscription subscription = PubsubSubscription.of(subscriptionName);
        if (subscription==null) throw new IllegalArgumentException(String.format("Subscription name '%s' not found", subscriptionName));

        return new EngineSubscription(subscription);
    }

}
