package io.epoxi.app.etlengine.api;


import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.epoxi.app.engine.queue.PullSubscriptionTask;
import io.epoxi.app.engine.queue.PushSubscriptionTask;
import io.epoxi.app.repository.AccountRepository;
import io.epoxi.app.repository.MessageQueueRepository;
import io.epoxi.app.repository.StepEndpointTemplateRepository;
import io.epoxi.app.repository.model.MessageQueue;
import io.epoxi.app.repository.model.StepEndpointTemplate;
import io.epoxi.app.repository.modelbase.EngineRepository;

import java.util.List;
import java.util.Timer;

public class EtlEngineApiController implements ETLEngineApi {

    private final AccountRepository accountRepository;

    public EtlEngineApiController() {
        super(); // init the ApiBase
        accountRepository = new AccountRepository();
    }

    public void addStepEndpointTemplate(StepEndpointTemplate stepEndpointTemplate) {
        StepEndpointTemplateRepository stepEndpointTemplateRepository = accountRepository
                .getStepEndpointTemplateRepository();
        stepEndpointTemplateRepository.add(stepEndpointTemplate);
    }

    public void deleteStepEndpointTemplate(Long id) {
        deleteStepEndpointTemplate(id, false);
    }

    public void deleteStepEndpointTemplate(Long id, Boolean permanent) {
        StepEndpointTemplateRepository stepEndpointTemplateRepository = accountRepository
                .getStepEndpointTemplateRepository();
        stepEndpointTemplateRepository.delete(id, permanent);
    }

    public StepEndpointTemplate getStepEndpointTemplate(Long id) {
        StepEndpointTemplateRepository stepEndpointTemplateRepository = accountRepository
                .getStepEndpointTemplateRepository();
        return stepEndpointTemplateRepository.get(id);
    }

    public void addMessageQueue(MessageQueue messageQueue) {
        MessageQueueRepository messageQueueRepository = accountRepository.getMessageQueueRepository();
        messageQueueRepository.add(messageQueue);
    }

    public void deleteMessageQueue(Long id) {
        deleteMessageQueue(id, false);
    }

    public void deleteMessageQueue(Long id, Boolean permanent) {
        MessageQueueRepository messageQueueRepository = accountRepository.getMessageQueueRepository();
        messageQueueRepository.delete(id, permanent);
    }

    public MessageQueue getMessageQueue(Long id) {
        MessageQueueRepository messageQueueRepository = accountRepository.getMessageQueueRepository();
        return messageQueueRepository.get(id);
    }


    public MessageQueue getMessageQueue(String name) {
        MessageQueueRepository messageQueueRepository = accountRepository.getMessageQueueRepository();
        return messageQueueRepository.get(name);
    }

    public StepEndpointTemplate getStepEndpointTemplate(String name) {
        StepEndpointTemplateRepository stepEndpointTemplateRepository = accountRepository
                .getStepEndpointTemplateRepository();
        return stepEndpointTemplateRepository.get(name);
    }


    @SuppressWarnings("unchecked")
    public List<StepEndpointTemplate> searchStepEndpointTemplate(String searchPattern, String name,  Integer skip,
             Integer limit) {

        EngineRepository<StepEndpointTemplate> repository = accountRepository.getStepEndpointTemplateRepository();
        List<StepEndpointTemplate> list;

        if (name != null) {
            list = repository.searchName(name);
        } else {
            list = repository.searchPattern(searchPattern);
        }

        return (List<StepEndpointTemplate>) limitList(list, skip, limit);
    }

    @SuppressWarnings("unchecked")
    public List<MessageQueue> searchMessageQueue(String searchPattern, String name,  Integer skip,  Integer limit) {

        EngineRepository<MessageQueue> repository = accountRepository.getMessageQueueRepository();
        List<MessageQueue> list;

        if (name != null) {
            list = repository.searchName(name);
        } else {
            list = repository.searchPattern(searchPattern);
        }

        return (List<MessageQueue>) limitList(list, skip, limit);
    }

    public void receiveMessage(String subscriptionName, String message, Boolean runAsync) {
        
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubSubMessage = PubsubMessage.newBuilder().setData(data).build();
        PushSubscriptionTask task = PushSubscriptionTask.of(subscriptionName, pubSubMessage);
        task.setRunAsync(runAsync);
        if (Boolean.FALSE.equals(runAsync))
        {            
             task.run();
        }
        else
        {
            new Timer().schedule(task, 0);
        }
    }

    public void receiveMessages(String subscriptionName, Boolean runAsync) {

        PullSubscriptionTask task = PullSubscriptionTask.of(subscriptionName);
        task.setRunAsync(runAsync);

        if (Boolean.FALSE.equals(runAsync))
        {            
            task.run();
        } 
        else
        {
            new Timer().schedule(task, 0); 
        }        
    }

	public List<MessageQueue> getMessageQueues() {
		return searchMessageQueue(null, null, 0, 0);
	}

    /**
     * @param list the list to limit
     * @param skip If 0 is passed, no records are skipped
     * @param limit If 0 is passed, no records are skipped
     * @return the specified list, limited by skip and limit
     */
    protected List<?> limitList(List<?> list, Integer skip, Integer limit)
    {
        if (list.size() <= skip) return list;

        int end = skip + limit ;

        //If no limit was passed, return it all
        if (limit==0) end = list.size() - skip;

        if (list.size() <= end) end = list.size();

        return list.subList(skip, end);
    }
}