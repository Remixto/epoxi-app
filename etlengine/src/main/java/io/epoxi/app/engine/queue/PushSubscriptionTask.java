package io.epoxi.app.engine.queue;

import com.google.pubsub.v1.PubsubMessage;
import io.epoxi.app.engine.queue.action.Action;
import io.epoxi.app.engine.queue.action.ActionReceiver;
import io.epoxi.app.cloud.logging.AppLog;
import org.slf4j.Marker;

import java.lang.invoke.MethodHandles;


/**
 * An asynchronous task that:
 * 1) receives a message from a (PUSH) pubsub subscription and builds an ActionReceiver from the message received
 * 2) calls the Run method on the ActionReceiver to process the action
 */
public class PushSubscriptionTask extends SubscriptionTaskBase {

    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

    final EngineSubscription subscription;

    private PushSubscriptionTask(EngineSubscription engineSubscription, PubsubMessage message)
    {
        Marker marker = logger.logEntry("Constructor");

        this.subscription = engineSubscription;
        populateActionReceivers(message);

        logger.atDebug().addKeyValue("subscription", engineSubscription.getName()).log("PushSubscriptionTask object initialized");
        logger.logExit(marker);
    }

    private void populateActionReceivers(PubsubMessage message)
    {
        Action action = Action.fromMessage(subscription.getStepType(), message);
        ActionReceiver currentReceiver = subscription.getMessageReceiver(action);

        if (currentReceiver == null)
        {
            logger.atWarn().log("A receiver cannot be determined for the action '{}'", action.getKeysetId());
            return;
        }

        receivers.add(currentReceiver);
    }

    public static PushSubscriptionTask of(String subscriptionName, PubsubMessage message)
    {
        EngineSubscription engineSubscription = EngineSubscription.of(subscriptionName);
        return of(engineSubscription, message);
    }

    public static PushSubscriptionTask of(EngineSubscription engineSubscription, PubsubMessage message)
    {
        return new PushSubscriptionTask(engineSubscription, message);
    }
}
