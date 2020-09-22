package io.epoxi.app.engine.etl;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.epoxi.app.cloud.pubsub.*;
import io.epoxi.app.engine.etl.pipeline.EtlPipeline;
import io.epoxi.app.engine.etl.pipeline.PipelineEndpoint;
import io.epoxi.app.engine.etl.pipeline.PipelineException;
import io.epoxi.app.engine.queue.action.Action;
import io.epoxi.app.engine.queue.action.ActionBuffer;
import io.epoxi.app.engine.queue.action.ActionReceiver;
import io.epoxi.app.cloud.bq.BqService;
import io.epoxi.app.cloud.logging.AppLog;
import io.epoxi.app.cloud.logging.StatusCode;
import io.epoxi.app.repository.model.*;
import io.epoxi.cloud.pubsub.*;
import io.epoxi.app.engine.Config;
import io.epoxi.engine.queue.action.*;
import io.epoxi.app.repository.AccountRepository;
import io.epoxi.app.repository.MessageQueueRepository;
import io.epoxi.app.repository.StepEndpointTemplateRepository;
import io.epoxi.app.repository.event.Event;
import io.epoxi.app.repository.event.EventStream;
import io.epoxi.repository.model.*;
import io.epoxi.app.repository.model.MessageQueue.QueueType;
import lombok.Getter;
import org.slf4j.Marker;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.EnumMap;
import java.util.List;
import java.util.Map.Entry;

/**
 *  A step in an ETL process for a job.
 */
public class ETLStep implements ActionReceiver {

  private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

  @Getter
  private final Ingestion ingestion;  //A pointer back to the job that spawned the step

  @Getter
  private final StepType stepType;

  @Getter
  private final EtlPipeline pipeline;

  @Getter
  private final EnumMap<StepEndpointTemplate.EndpointType, PipelineEndpoint> pipelineEndpoints;

  @Getter
  private final EnumMap<QueueType, String> messages;

  /***
   * Create an ETLStep based on the specified ingestion and stepType
   * @param ingestion The ingestion to be used to create the ETLStep
   * @param stepType The stepType of the resulting ETLStep
   */
  public ETLStep(Ingestion ingestion, StepType stepType) {

      Marker marker = logger.logEntry("Constructor");

      this.ingestion = ingestion;
      this.stepType = stepType;
      this.pipelineEndpoints = getEndpoints(ingestion.getAccountId());    //The source and target for the step
      this.messages = getStepTemplateMessages(ingestion.getAccountId());  //in, out, error messages locations (pubsub queues)

      this.pipeline = buildPipeline();

      logger.atDebug().log("ETLStep created from Json for keysetId '{}'", ingestion.getKeysetId());
      logger.logExit(marker);
  }

  @Override
  public void run() {

      try {
        pipeline.run();
        logStepResults();
      }
      catch (PipelineException ex) {
        logger.atError().addException(ex).log("Error in Pipeline. {}", ex.getMessage());
        //this error is not fatal as the ETLStep will be re-queued.
      }
      catch (ETLException ex) {
        logger.atError().addException(ex).log("Unexpected error in ETLStep. {}", ex.getMessage());
        //this error (while it should be investigated by developers) is also not fatal as the ETLStep will be re-queued.
      }
  }



  public KeysetId getKeysetId() {
    return ingestion.getKeysetId();
  }

  public void bufferAction(Action action)
  {
    bufferAction(action, null);
  }

  @Override
  public void bufferAction(Action action, AckReplyConsumer consumer) {

    try
    {
      logger.atDebug().log("Receiving action for keyset '{}' ", action.getKeysetId());

      //Buffer the action on the pipeline
      pipeline.getActionBuffer().add(action);
      if (consumer !=null)  consumer.ack();

      logger.atDebug().log("Processing complete on action received by ETLStep '{}'", this.getName());
    }
    catch(Exception ex)
    {
      //Only nack when there is an unhandled exception.
      //All other exceptions should be handled by the requeue (and so an ack is ok)
      if (consumer !=null)  consumer.nack();
    }
  }

  public void eom()
  {
    if (pipeline.getActionBuffer().getSize()> 0)
    {
      run();
    }
  }

  @Override
  public ActionBuffer getActionBuffer() {

    return pipeline.getActionBuffer();
  }

  @Override
  public Integer getNumActions() {

    return pipeline.getActionBuffer().getMaxSize();
  }

  @Override
  public Integer getNumMaxErrors() {

    return pipeline.getNumMaxErrors();
  }

  @Override
  public Integer getNumErrors() {

    return pipeline.getNumErrors();
  }

  @Override
  public String getName() {
    return String.format("%s:%s", getStepType().toString(), ingestion.getName());
  }

  /*
      * Replace substitution values in the template with data values from the job
  */
  private EnumMap<StepEndpointTemplate.EndpointType, PipelineEndpoint> getEndpoints(Long accountId) {

      logger.atDebug().log("Inflating PipelineEndpoints for Step '{}'", getName());

      EnumMap<StepEndpointTemplate.EndpointType, PipelineEndpoint> endpoints = new EnumMap<>(StepEndpointTemplate.EndpointType.class);

      StepEndpointTemplateRepository stepEndpointTemplateRepository = AccountRepository.of(accountId).getStepEndpointTemplateRepository();
      List<StepEndpointTemplate> endpointTemplates = stepEndpointTemplateRepository.search(stepType);

      for(StepEndpointTemplate endpointTemplate : endpointTemplates)
      {
        TableId tableId = substituteValues(BqService.getTableId(endpointTemplate.getTableId()));
        String keyFieldId = substituteValues(endpointTemplate.getKeyFieldId());

        PipelineEndpoint endpoint = new PipelineEndpoint(tableId, keyFieldId);
        endpoints.put(endpointTemplate.getEndpointType(), endpoint);
      }

      logger.atDebug().log("Inflation completed for Step '{}'", getName());

      return endpoints;
  }

   /**
   * Configure messaging lists within each template with data values from the job
   */
  private EnumMap<QueueType, String> getStepTemplateMessages(Long accountId) {

      logger.atDebug().log("Inflating MessagePaths for Step '{}'", getName());

      EnumMap<QueueType, String> templateMessages = new EnumMap<>(QueueType.class);

      MessageQueueRepository messageQueueRepository = AccountRepository.of(accountId).getMessageQueueRepository();
      List<MessageQueue> queues = messageQueueRepository.search(stepType);
      for(MessageQueue messageQueue : queues)
      {
        Queue queue = messageQueue.getQueue();
        String path = queue.getPath();
        String newPath = substituteValues(path);

        templateMessages.put(messageQueue.getQueueType(), newPath);
      }

      logger.atDebug().log("Inflation completed for Step '{}'", getName());
      return templateMessages;
  }

  private String substituteValues(String value) {

      if (value == null)
          return null;

      TableId source = ingestion.getSource().getTableId();
      TableId target = ingestion.getTarget().getTableId();

      String afterValue = value;
      afterValue = afterValue.replace("{keysetId}", getKeysetId().toString());
      afterValue = afterValue.replace("{keysetName}", getKeysetId().getKeysetName());
      afterValue = afterValue.replace("{projectName}", getKeysetId().getProjectName());
      afterValue = afterValue.replace("{source.tableId}", BqService.getQualifiedTable(source));
      afterValue = afterValue.replace("{target.tableName}", target.getTable());
      afterValue = afterValue.replace("{target.tableId}", BqService.getQualifiedTable(target));

      logger.atDebug().addKeyValue("before", value).addKeyValue("after", afterValue).log("Value substitution was completed");
      return afterValue;
  }

  private TableId substituteValues(TableId tableId) {
      String value = BqService.getQualifiedTable(tableId);

      String newValue = substituteValues(value);
      if (newValue == null) return null;

      return BqService.getTableId(newValue.split("\\."));
  }

  private EtlPipeline buildPipeline()
  {
      return EtlPipeline.newBuilder()
                .withStepType(stepType)
                .withIngestion(ingestion)
                .withEndpoints(pipelineEndpoints)
                .build();
  }

  private void logStepResults() {

      TopicName topicName = null;

      try {
            for (Entry<QueueType, List<Event>> entry : getPipeline().getResultEvents().entrySet()) {

              //Determine what topic we will be sending messages to
              topicName = PubsubTopic.toTopicName(getMessages().get(entry.getKey()));
              logStepResult(entry, topicName);

            logger.atDebug().log("Result events written to  '{}' for keysetId '{}'", topicName.getTopic(), ingestion.getKeysetId());
          }
      }
      catch (PubsubException ex) {

        String msg = "Cannot obtain event stream based on Pubsub topic";
        if (topicName!=null)
          msg = String.format("'%s'", topicName.getTopic());

        throw new ETLException(msg, this, ex, ex.getStatusCode());
      }
  }

private void logStepResult(Entry<QueueType, List<Event>> entry, TopicName topicName)
{
  PubsubTopic topic;

  try
  {
    topic = PubsubTopic.of(topicName);

    //If the topic does not exist, create it (along with a default subscriber)
    if (topic == null)
    {
      PubsubTopic.createTopic(topicName);

       //create a subscription for the new topic
      final ProjectSubscriptionName subscriptionName = PubsubTopic.getDefaultSubscriptionName(topicName);
      PubsubSubscription.createSubscription(topicName, subscriptionName, Config.PUBSUB_ACK_DEADLINE_SECONDS);

      topic = PubsubTopic.of(topicName);
    }

    //Make a message stream object and populate it with the messages returned from the pipeline
    List<Event> resultMessages = entry.getValue();

    EventStream stream = new EventStream();
    stream.getEvents().addAll(resultMessages);
    stream.setWriter(topic.getWriter());

    //Write the messages to the topic
    PubsubWriter writer = stream.getWriter();
    List<Writable> writeables = stream.getEvents();

    writer.write(writeables.toArray(new Writable[writeables.size()]));
  }
  catch(IOException ex)
  {
    String msg = String.format("Cannot create default Subscription for the topic '%s'", topicName);
    throw new ETLException(msg, ex, StatusCode.UNAVAILABLE);
  }
  catch (InterruptedException ex)
  {
    //Log and return nothing
    Thread.currentThread().interrupt();
    logger.atWarn().log("Step execution interrupted");
  }
}


}
