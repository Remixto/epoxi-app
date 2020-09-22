package io.epoxi.repository;

import io.epoxi.repository.model.MessageQueue;
import io.epoxi.repository.model.StepType;
import io.epoxi.repository.modelbase.EngineRepository;
import io.epoxi.repository.modelbase.ObjectifyRegistry;

import java.util.List;

public class MessageQueueRepository extends EngineRepository<MessageQueue> {

	protected MessageQueueRepository() {
		super(MessageQueue.class);
	}	

	public List<MessageQueue> search(MessageQueue.QueueType queueType) {

		return ObjectifyRegistry.run(() -> ObjectifyRegistry.ofy().load().type(MessageQueue.class).filter("queueType =", queueType).list());

	}

	public List<MessageQueue> search(StepType stepType) {
        
		return ObjectifyRegistry.run(() -> ObjectifyRegistry.ofy().load().type(MessageQueue.class).filter("stepType =", stepType).list());

	
	}



	
}