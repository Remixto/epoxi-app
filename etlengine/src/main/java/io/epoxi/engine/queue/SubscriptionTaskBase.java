package io.epoxi.engine.queue;

import io.epoxi.engine.queue.action.ActionReceiver;
import io.epoxi.cloud.logging.AppLog;
import io.epoxi.cloud.logging.StatusCode;
import io.epoxi.cloud.pubsub.PubsubException;
import io.epoxi.engine.Config;
import io.epoxi.engine.EngineException;
import lombok.Getter;
import lombok.Setter;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A task that:
 * 1) reads messages from a (PULL) pubsub subscription and builds a collection of ActionReceivers from the messages read
 * 2) calls the Run method on the ActionReceivers to process each action
 */
public abstract class SubscriptionTaskBase extends TimerTask {

    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

    List<ActionReceiver> receivers = new ArrayList<>();

    @Getter @Setter
    Boolean runAsync = true;

    @Override
    public void run() {

        try
        {
            runActionReceivers();
        }
        catch (PubsubException ex)
        {
            logger.atError().addException(ex).log("PubsubException encountered");
        }
    }

    private void runActionReceivers()
    {
        try{
            ExecutorService es = Executors.newCachedThreadPool();
            for(ActionReceiver receiver : receivers) {

                logger.atInfo().log("ActionReceiver '{}'' processing started.", receiver.getName());

                if (Boolean.TRUE.equals(runAsync))
                {
                    es.execute(() -> {
                        /* do the work to be done in its own thread */
                        try
                        {
                            receiver.run();
                        }
                        catch (Exception ex)
                        {
                            //Log and continue.  All receivers must run because all are responsible for processing messages and requesting on failure
                            logger.atError().addException(ex).log("ActionReceiver '{}'' failed during run with message '{}'", receiver.getName(), ex.getMessage());
                        }
                    }
                    );
                }
                else
                {
                    receiver.run();
                }
            }
            es.shutdown();
            es.awaitTermination(Config.RECEIVER_RUN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            // all tasks have finished or the time has been reached.
        }
        catch(InterruptedException ex)
        {
            Thread.currentThread().interrupt();
            throw new EngineException("Error getting query result", StatusCode.CANCELLED);
        }
    }

}
