package io.epoxi.engine.etl.pipeline;

import io.epoxi.engine.queue.action.ActionBuffer;
import io.epoxi.util.validation.Violation;
import io.epoxi.cloud.bq.BqService;
import io.epoxi.cloud.logging.AppLog;
import io.epoxi.repository.event.ETLTimestamp;
import io.epoxi.repository.event.Event;
import io.epoxi.repository.model.Ingestion;
import io.epoxi.repository.model.Ingestion.ReplicationType;
import io.epoxi.repository.model.KeysetId;
import io.epoxi.repository.model.MessageQueue.QueueType;
import io.epoxi.repository.model.StepEndpointTemplate;
import io.epoxi.repository.model.StepType;
import io.epoxi.util.validation.Validator;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.lang.invoke.MethodHandles;
import java.util.*;

public abstract class EtlPipeline {

    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

    // Fields used in the running of the pipeline
    @Getter
    @Setter
    private PipelineEndpoint sourceEndpoint;

    @Getter
    @Setter
    private PipelineEndpoint targetEndpoint;

    @Getter
    @Setter
    private ETLTimestamp timestamp;

    protected BqService bqService;

    // Field to hold the actionBuffer
    @Getter
    ActionBuffer actionBuffer = new ActionBuffer();

    @Getter(AccessLevel.PROTECTED)
    Validator validator = new Validator();

    // Fields used for Results processing and Messaging
    private final Map<QueueType, List<Event>> resultEvents = new EnumMap<>(QueueType.class);

    @Getter
    @Setter(AccessLevel.PROTECTED)
    private Integer numMaxErrors = 3;

    @Getter
    private Integer numErrors = 0;

    public abstract StepType getStepType();

    /**
     * Run the pipeline on the buffer. If an error occurs the error message queue
     * will be populated with a requeue action and a PipelineException will be
     * thrown. The buffer will be emptied after the run, regardless of the result.
     */
    public abstract void run();

    protected EtlPipeline(KeysetId keysetId) {
      actionBuffer.setKeysetId(keysetId);
      resultEvents.put(QueueType.OUT, new ArrayList<>());
      resultEvents.put(QueueType.ERROR, new ArrayList<>());
    }

    public List<Violation> validate() {
      return validator.validate();
    }

    public void validOrThrow() {
      validator.validOrThrow(this.getKeysetId().toString());
    }

    protected void setEndpoints(Map<StepEndpointTemplate.EndpointType, PipelineEndpoint> pipelineEndpoints) {
      setSourceEndpoint(pipelineEndpoints.get(StepEndpointTemplate.EndpointType.SOURCE));
      setTargetEndpoint(pipelineEndpoints.get(StepEndpointTemplate.EndpointType.TARGET));
    }

    public KeysetId getKeysetId() {
      return actionBuffer.getKeysetId();
    }

    public void setKeysetId(KeysetId keysetId) {
      actionBuffer.setKeysetId(keysetId);
    }

    public ReplicationType getUpdateType() {
      return actionBuffer.getUpdateType();
    }

    public Map<QueueType, List<Event>> getResultEvents() {
      return Collections.unmodifiableMap(resultEvents);
    }

    protected void incrementError() {
      numErrors++;
    }

    public static EtlPipeline.Builder newBuilder() {
      return stepType -> ingestion -> endpoints -> new OptionalsBuilder().setStepType(stepType).setIngestion(ingestion).setPipelineEndpoints(endpoints);
    }


    /******************************************
     * Builder.  A functional interface with a single method (withName) that starts the build.
     */

    @FunctionalInterface
    public interface Builder {

        IngestionBuilder withStepType(final StepType stepType);

        interface IngestionBuilder {
          EndpointsBuilder withIngestion(final Ingestion ingestion);
        }

        interface EndpointsBuilder {
            OptionalsBuilder withEndpoints(final Map<StepEndpointTemplate.EndpointType, PipelineEndpoint> pipelineEndpoints);
        }
    }

    public static class OptionalsBuilder {

      @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) Map<StepEndpointTemplate.EndpointType, PipelineEndpoint> pipelineEndpoints;
      @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) Ingestion ingestion;
      @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) StepType stepType;

      @Accessors(chain = true) @NonNull @Setter() ETLTimestamp etlTimestamp;

      public EtlPipeline build() {

        EtlPipeline pipeline = getPipeline();

        if (pipeline.actionBuffer.getKeysetId() == null)
          pipeline.actionBuffer.setKeysetId(ingestion.getKeysetId());

        pipeline.setTimestamp(etlTimestamp);

        //Validate the pipeline
        pipeline.validator.validOrThrow(ingestion.getKeysetId().toString());

        logger.atDebug().log("A {} pipeline has been built for '{}'", stepType, pipeline.getKeysetId());
        return pipeline;

      }

      private EtlPipeline getPipeline()
      {
        EtlPipeline pipeline;

        try
        {
            //Create the pipeline object
            switch (stepType) {
                case EXTRACT:
                    pipeline = ExtractPipeline.of(ingestion, pipelineEndpoints);
                    break;
                case TRANSFORM:
                    pipeline = TransformPipeline.of(ingestion, pipelineEndpoints);
                    break;
                case LOAD:
                    pipeline = LoadPipeline.of(ingestion, pipelineEndpoints);
                    break;
                default:
                    throw new IllegalArgumentException(
                        "Error encountered while building Pipeline.  Pipeline could not be created from the values provided.");
            }

            return pipeline;
        }
        catch(Exception ex)
        {
             throw new IllegalArgumentException(
                 "Error encountered while building Pipeline.  Pipeline could not be created from the values provided.");
        }

      }

    }
}
