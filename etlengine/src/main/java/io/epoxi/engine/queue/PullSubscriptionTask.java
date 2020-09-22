package io.epoxi.engine.queue;

import io.epoxi.cloud.logging.AppLog;
import org.slf4j.Marker;

import java.lang.invoke.MethodHandles;


/**
 * An asynchronous task that:
 * 1) reads messages from a (PULL) pubsub subscription and builds a collection of ActionReceivers from the messages read
 * 2) calls the Run method on the ActionReceivers to process each action
 */
public class PullSubscriptionTask extends SubscriptionTaskBase {

    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

    final PullSubscriptionReader subscriptionReader;

    private PullSubscriptionTask(EngineSubscription engineSubscription)
    {
        Marker marker = logger.logEntry("constructor");

        this.subscriptionReader = new PullSubscriptionReader(engineSubscription);
        populateActionReceivers();

        logger.atDebug().addKeyValue("subscription", subscriptionReader.subscription.getName()).log("PullSubscriptionTask object initialized");
        logger.logExit(marker);
    }

    public void populateActionReceivers()
    {
        receivers = subscriptionReader.receiveMessages();
    }

    public static PullSubscriptionTask of(String subscriptionName)
    {
        EngineSubscription engineSubscription = EngineSubscription.of(subscriptionName);
        return of(engineSubscription);
    }

    public static PullSubscriptionTask of(EngineSubscription engineSubscription)
    {
        return new PullSubscriptionTask(engineSubscription);
    }
}
