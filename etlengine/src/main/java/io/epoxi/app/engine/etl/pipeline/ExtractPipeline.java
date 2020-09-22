package io.epoxi.app.engine.etl.pipeline;

import io.epoxi.app.cloud.bq.BqException;
import io.epoxi.app.cloud.bq.BqService;
import io.epoxi.app.cloud.logging.AppLog;
import io.epoxi.app.cloud.logging.StatusCode;
import io.epoxi.app.engine.Config;
import io.epoxi.app.engine.queue.action.Action;
import io.epoxi.app.repository.event.ETLTimestamp;
import io.epoxi.app.repository.event.Event;
import io.epoxi.app.repository.event.EventKey;
import io.epoxi.app.repository.model.*;
import io.epoxi.app.util.sqlbuilder.ETLBuilder;
import io.epoxi.cloud.bq.*;
import io.epoxi.engine.queue.action.*;
import io.epoxi.app.source.DataReplicator;
import io.epoxi.app.repository.model.Ingestion;
import io.epoxi.app.repository.model.KeysetId;
import io.epoxi.app.repository.model.StepEndpointTemplate;
import io.epoxi.app.repository.model.StepType;
import io.epoxi.app.repository.model.Stream;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.TableResult;

import org.slf4j.Marker;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ExtractPipeline extends EtlPipeline {

    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

    private final String sourceTableKeyFieldId;
    private final Integer etlExtractTableExpirationSeconds;
    private final Stream stream;

    private ExtractPipeline(Ingestion ingestion, Map<StepEndpointTemplate.EndpointType, PipelineEndpoint> pipelineEndpoints)
    {
        super(ingestion.getKeysetId());
        Marker marker = logger.logEntry("Constructor");

        KeysetId keysetId = ingestion.getKeysetId();
        bqService = new BqService(keysetId.getProjectName());

        //Configure the pipeline endpoints
        setEndpoints(pipelineEndpoints);

        //Configure other pipeline settings
        setNumMaxErrors(1);
        setDeleteThreshold(ingestion.getDeleteThreshold());
        stream = ingestion.getStreams().get(0);
        sourceTableKeyFieldId = ingestion.getKeyFieldId();
        etlExtractTableExpirationSeconds = Config.ETL_EXTRACT_TABLE_EXPIRATION_SECONDS;

        logger.atDebug()
            .addKeyValue("Source", pipelineEndpoints.get(StepEndpointTemplate.EndpointType.SOURCE))
            .addKeyValue("Target", pipelineEndpoints.get(StepEndpointTemplate.EndpointType.TARGET))
            .log("ExtractPipeline object initiated from builder");

        logger.logExit(marker);

    }

    @Override
    public StepType getStepType() {

        return StepType.EXTRACT;
    }

    public void run()
    {
        Marker marker = logger.logEntry("run");

        //As the maxBufferSize for LoadPipelines is 1, the action to be processed during
        //run will be the first (and only) items in the buffer
        logger.atInfo().log("ETLExtract started for '{}'", getKeysetId());

        try
        {
            //This is the timestamp for the current action (it is new and based on now)
            if (getTimestamp()==null) setTimestamp(ETLTimestamp.now());

            // Copy the source data to the extract table (overwriting old data)
            replicateSourceData();

            // Generate the CDC data (and return the result)
            generateCDC();

            // Iterate through the result set and publish Pub/Sub messages (in JSON form) to the topic Extract_CDC based for each row
            logOutEvents();

            logger.atInfo().log("ETLExtract completed for '{}'", getKeysetId());
        }
        catch (Exception ex)
        {
            incrementError();
            logErrorEvents();
            String msg = String.format("%s Pipeline Error: %s", getStepType(), ex.getMessage());
            throw new PipelineException(msg, this, ex, StatusCode.INTERNAL);
        }
        finally
        {
            getActionBuffer().clear();
        }

        logger.logExit(marker);
    }

    private void replicateSourceData()
    {
        logger.atInfo().log("ETLExtract.replicateSourceData started for '{}'", getKeysetId());

        DataReplicator replicator = DataReplicator.newBuilder()
                                                    .setStream(stream)
                                                    .setTargetTable(getTargetEndpoint().getTableId())
                                                    .setPipelineTempDirectory(Config.PIPELINE_TEMP_DIRECTORY)
                                                    .setTargetTableExpiration(etlExtractTableExpirationSeconds)
                                                    .build();

        try
        {
            replicator.run();  //This will push data to the sourceTable
            logger.atDebug().log("Data has been replicated from Table '{}' to '{}'", getSourceEndpoint().getQualifiedTable(), getTargetEndpoint().getQualifiedTable());
        }
        catch (BqException ex) {
            String msg = String.format("Error replicating Source data for keyset '%s'", this.getKeysetId());
            throw new PipelineException(msg, this, ex, StatusCode.INTERNAL);
        }

    }

    private void generateCDC()
    {

        logger.atInfo().log("ETLExtract.GenerateCDC started for '{}'", getKeysetId());

        // Do the merge to update the CDC table
        String query =  ETLBuilder.getETLExtractGenerateCDCStatement();

        Map<String, QueryParameterValue> parameters = new HashMap<>();
        parameters.put("table_Id", QueryParameterValue.string(getTargetEndpoint().getQualifiedTable()));
        parameters.put("keyset_name", QueryParameterValue.string(getKeysetId().toString()));
        parameters.put("keyfield_Id", QueryParameterValue.string(sourceTableKeyFieldId));
        parameters.put("etl_update_type", QueryParameterValue.string(getUpdateType().toString()));
        parameters.put("cdc_timestamp", QueryParameterValue.timestamp(getTimestamp().toNanos()));
        parameters.put("delete_threshold", QueryParameterValue.float64(getDeleteThreshold()));

        try
        {
            bqService.executeQuery(query, parameters);
            logger.atDebug().log("A CDC Merge has been performed on Table '{}' to '{}'", getSourceEndpoint().getQualifiedTable(), getTargetEndpoint().getQualifiedTable());
        }
        catch (BqException ex) {
            String msg = String.format("Error generating the CDC data from the new Source data for keyset '%s'", this.getKeysetId());
            throw new PipelineException(msg, this, ex, StatusCode.INTERNAL);
        }
    }

    /**
     * Set the OUT Messages for publication back to PubSub.  For ExtractPipelines, one message
     * is created for each CDC action that results from the pipeline action
    */

    private void logOutEvents()
    {
        try
        {
            //Create a list that will hold the CDC actions
            List<Event> cdcEvents =  new ArrayList<>();

            // Get the merge result that we will push to pub/sub
            String query = ETLBuilder.getETLExtractGenerateCDCResultStatement();

            Map<String, QueryParameterValue> parameters = new HashMap<>();
            parameters.put("keyset_name", QueryParameterValue.string(getKeysetId().toString()));
            parameters.put("cdc_timestamp", QueryParameterValue.timestamp(getTimestamp().toNanos()));

            TableResult result = bqService.getQueryResult(query, parameters);

            // Populate the OUT action list with results from the query
            ETLTimestamp timestamp = null;
            Event event = new Event(getKeysetId());
            List<EventKey> eventKeys = event.getEventKeys();

            for (FieldValueList row : result.iterateAll()) {
                if(timestamp == null)
                {
                    timestamp = ETLTimestamp.fromNanos(row.get("Last_Timestamp").getTimestampValue());
                }

                EventKey eventKey = new EventKey();
                eventKey.setEvent(EventKey.toEventEnum(row.get("Last_CDC_Action").getStringValue()));
                eventKey.setKey(Long.parseLong(row.get("Key").getStringValue()));

                eventKeys.add(eventKey);
            }

            event.setEventTimestamp(timestamp);
            cdcEvents.add(event);

            //Add the Events to the Results collection for the pipeline
            getResultEvents().get(MessageQueue.QueueType.OUT).addAll(cdcEvents);

        }
        catch(BqException ex)
        {
            throw new PipelineException("Error generating a list of CDC events for OutEvents", this, ex, StatusCode.INTERNAL);
        }
    }

    private void logErrorEvents() {

        //If we get here, something went wrong in the processing of the ETL pipeline
        //and we should simply requeue the action in the buffer
        Event masterEvent = new Event(getKeysetId());

        for(Action action : actionBuffer)
        {
            masterEvent.getEventKeys().addAll(action.getEvent().getEventKeys());
        }

        getResultEvents().get(MessageQueue.QueueType.ERROR).add(masterEvent);
    }

    protected static EtlPipeline of(Ingestion ingestion, Map<StepEndpointTemplate.EndpointType, PipelineEndpoint> pipelineEndpoints)
    {
        ExtractPipeline pipeline = new ExtractPipeline(ingestion, pipelineEndpoints);

        //Configure validation
        pipeline.validator.addAll(pipeline.getSourceEndpoint().validator);

        return pipeline;
    }

    @Getter @Setter
    private float deleteThreshold;

}
