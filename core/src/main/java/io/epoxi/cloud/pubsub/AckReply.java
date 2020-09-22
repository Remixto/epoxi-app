package io.epoxi.cloud.pubsub;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.ReceivedMessage;

public class AckReply implements AckReplyConsumer {

    Boolean reply;
    final String messageId;

    public AckReply(ReceivedMessage message){
        this.messageId = message.getAckId();
    }

    public AckReply(String messageId){
        this.messageId = messageId;
    }

    @Override
    public void ack() {
       reply = true;
    }

    @Override
    public void nack() {
        reply = false;
    }

    public String getAckId()
    {
        return messageId;
    }

    public Boolean getAckReply()
    {
        return reply;
    }

}
