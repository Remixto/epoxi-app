package io.epoxi.app.engine.queue.action;
import com.google.cloud.pubsub.v1.AckReplyConsumer;

public interface ActionReceiver {

   ActionBuffer getActionBuffer();

   /**
    * The max number of actions to receive
    */
   Integer getNumActions();

   Integer getNumMaxErrors();
   Integer getNumErrors();

   void bufferAction(Action action, AckReplyConsumer consumer);

   void run();
   void eom();
   String getName();
}
