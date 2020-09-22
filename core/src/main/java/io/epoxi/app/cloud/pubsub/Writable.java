package io.epoxi.app.cloud.pubsub;

import com.google.pubsub.v1.PubsubMessage;

public interface Writable {
    PubsubMessage toMessage();
}
