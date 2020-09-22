package io.epoxi.engine.queue;

import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import io.epoxi.engine.queue.action.Action;
import io.epoxi.engine.queue.action.ActionException;
import io.epoxi.engine.queue.action.ActionReceiver;
import io.epoxi.cloud.logging.AppLog;
import io.epoxi.cloud.logging.StatusCode;
import io.epoxi.cloud.pubsub.AckReply;
import io.epoxi.engine.Config;
import io.epoxi.engine.EngineException;
import org.slf4j.Marker;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

/**
 * Allows reading of messages from a (PULL) PubSub Subscription.  Messages read from a subscription
 *  can be buffered into ActionReceiver object(s) that can action on the actions buffered.
 */
public class PullSubscriptionReader {

    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());
    public final EngineSubscription subscription;

    public PullSubscriptionReader(EngineSubscription subscription)
    {
        this.subscription = subscription;
    }

    /**
     *  Get messages from the PubSubSubscription.
     *  Buffer them into receivers.
     *  Once no more messages can be buffered, messages will be acked.
     *  After that, the Run method on each receiver will be run.
     */
    public List<ActionReceiver> receiveMessages() {

        Marker marker = logger.logEntry("receiveMessages");
        logger.atInfo().log("Receiving Messages for {}.", subscription.getName());

        //Try to get a receiver based on data in the subscription name. It is ok if we don't get one at this point
        final List<AckReply> ackIds = new ArrayList<>();
        List<ActionReceiver> receivers = new ArrayList<>();
        try (SubscriberStub subscriber = subscription.getSubscriberStub()) {

            //Initialize some counters for the method.
            Integer numMessagesToPull = Config.READ_NUM_MESSAGES_ON_PULL_SUBSCRIPTION;
            int numMessagesPulled = 0;
            boolean receiving = true;

            while (Boolean.TRUE.equals(receiving)) {
                /*
                 * Pull messages (the requested number)
                 */
                List<ReceivedMessage> messages = subscription.pullMessages(subscriber, numMessagesToPull);
                numMessagesPulled = messages.size();
                if (numMessagesPulled == 0) {
                    break;  //Exit because we got no messages
                }

                /*
                 * Buffer messages to the receiver.  If we pass a null receiver,
                 * we will get one back from the method.
                 */
                List<ActionReceiver> bufferedReceivers = bufferMessages(messages, ackIds);
                receivers.addAll(bufferedReceivers);

                //If we didn't get a full allocation of messages
                if (numMessagesPulled < numMessagesToPull) {
                    receiving = false;
                }
            }

            /*
             * Acknowledge messages
             */
            if (numMessagesPulled > 0) {
                //The receiver has stopped accepting message (and some messages were received).  Time to acknowledge them and exit
                subscription.acknowledgeMessages(subscriber, ackIds, true);
            }

            logger.logExit(marker);
            return receivers;
        } catch (Exception ex) {
            String msg = String.format("Error receiving messages from Subscription '%s'", subscription.getName());
            throw new EngineException(msg, ex, StatusCode.NOT_FOUND);
        }
    }

    /**
     * Returns a list of ActionReceivers containing the messages that have been buffered
     * @param messages The received messages that are to be buffered into ActionReceivers
     * @param ackIds a list of ack results for each received message
     * @return A list of ActionReceiver objects with populated actionBuffers based on the received messages
     */
    private List<ActionReceiver> bufferMessages(List<ReceivedMessage> messages, List<AckReply> ackIds)
    {
        List<ActionReceiver> bufferedReceivers = new ArrayList<>();

        try{
            ActionReceiver currentReceiver = null;

            //Process the messages
            for (ReceivedMessage message : messages) {

                //Build the reply for the message
                AckReply reply = new AckReply(message);
                ackIds.add(reply);

                //Convert the message to an event
                PubsubMessage msg = message.getMessage();
                Action action = Action.fromMessage(subscription.getStepType(), msg);

                if (action != null && currentReceiver == null)
                {
                    //Set the current receiver based on the action (typically occurs when processing the first action in the loop)
                    currentReceiver = subscription.getMessageReceiver(action);
                }
                else
                {
                    //We can't get an action, then nack and move onto the next message
                    logger.atWarn().addJson("pubsubMessage", msg.getData().toStringUtf8()).log("Pubsub message could not be converted into an valid action");
                    reply.nack();
                }

                if (currentReceiver == null)
                {
                    //We can't get a receiver, nack, log a warning and move onto the next message
                    if (action != null) logger.atWarn().log("A receiver cannot be determined for the action '{}'", action.getKeysetId());

                    reply.nack();
                    continue;
                }

                //At this point, we have an Action and a Receiver.  Lets buffer the action into the receiver
                currentReceiver.bufferAction(action, reply);

                //If the buffer is full, then add the currentReceiver to the bufferedReceivers and empty the currentReceiver
                if (Boolean.TRUE.equals(currentReceiver.getActionBuffer().isFull()))
                {
                    bufferedReceivers.add(currentReceiver);
                    logger.atDebug().log("The receiver '{}' has buffered a full allocation of message(s)", currentReceiver.getName());
                    currentReceiver = null;
                }
            }

            //Buffer the last receiver, if it is not yet null
            if (currentReceiver !=null)
            {
                bufferedReceivers.add(currentReceiver);
                logger.atDebug().log("The receiver '{}' has buffered some message(s)", currentReceiver.getName());
            }

            logger.atDebug().log("{} messages were buffered into {} receivers", messages.size(), bufferedReceivers.size());
            return bufferedReceivers;
        }
        catch (ActionException ex)
        {
            String msg = String.format("Error buffering messages from Subscription '%s'", subscription.getName());
            throw new EngineException(msg, ex, StatusCode.INTERNAL);
        }
    }

}
