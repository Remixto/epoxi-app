package io.epoxi.engine.etl.pipeline;


import com.google.cloud.bigquery.TableId;
import io.epoxi.engine.queue.action.Action;
import io.epoxi.cloud.bq.BqException;
import io.epoxi.cloud.bq.BqService;
import io.epoxi.cloud.logging.AppLog;
import io.epoxi.cloud.logging.StatusCode;
import io.epoxi.repository.event.ETLTimestamp;
import io.epoxi.repository.event.Event;
import io.epoxi.repository.model.Ingestion;
import io.epoxi.repository.model.KeysetId;
import io.epoxi.repository.model.MessageQueue.QueueType;
import io.epoxi.repository.model.StepEndpointTemplate;
import io.epoxi.repository.model.StepType;
import io.epoxi.util.sqlbuilder.ETLBuilder;
import io.epoxi.util.sqlbuilder.SqlBuilderException;
import io.epoxi.util.sqlbuilder.statement.CreateTableStatement;
import io.epoxi.util.sqlbuilder.statement.MergeStatement;
import io.epoxi.util.sqlbuilder.types.SqlTable;
import io.epoxi.util.sqlbuilder.types.SqlTableDef;
import org.slf4j.Marker;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class LoadPipeline extends EtlPipeline {

    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());
    String jobTargetTableKeyFieldId;

    private LoadPipeline(Ingestion ingestion, Map<StepEndpointTemplate.EndpointType, PipelineEndpoint> pipelineEndpoints)
    {
        super(ingestion.getKeysetId());
        Marker marker = logger.logEntry("Constructor");

        KeysetId keysetId = ingestion.getKeysetId();
        bqService = new BqService(keysetId.getProjectName());

        //Configure the pipeline endpoints
        setEndpoints(pipelineEndpoints);

        //Configure other pipeline settings
        setNumMaxErrors(1);
        jobTargetTableKeyFieldId = ingestion.getTarget().getKeyFieldId();

        logger.atDebug()
            .addKeyValue("Source", pipelineEndpoints.get(StepEndpointTemplate.EndpointType.SOURCE))
            .addKeyValue("Target", pipelineEndpoints.get(StepEndpointTemplate.EndpointType.TARGET))
            .log("LoadPipeline object initiated from builder");

        logger.logExit(marker);
    }



    @Override
    public StepType getStepType() {

        return StepType.LOAD;
    }

    @Override
    public void run() {

        //As the maxBufferSize for LoadPipelines is 1, the action to be processed during
        //run will be the first (and only) items in the buffer
        logger.atInfo().log("ETLLoad started for '{}'", getKeysetId());

        //Enable the validation that was previous delayed
        validator.setEnabled(true);

        try
        {
            //Get the timestamp for the action being processed and then validate the pipeline
            ETLTimestamp actionTimestamp = actionBuffer.get(0).getActionTimestamp();
            getSourceEndpoint().setTimestamp(actionTimestamp);
            validator.validOrThrow(getKeysetId().toString());

            //This is the timestamp for the current action (it is new and based on now)
            if (getTimestamp()==null) setTimestamp(ETLTimestamp.now());

            //Create the target table
            createStoreTable();

            //Perform the merge
            performMerge();

            //Log and cleanup
            logOutEvents();
            logger.atInfo().log("ETLLoad completed for '{}'", getKeysetId());

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
    }

    void performMerge()
    {
        logger.atInfo().log("ETLLoad.PerformMerge started for '{}'", getKeysetId());

        SqlTable sourceTable = SqlTable.of(getSourceEndpoint().getTableId());
        SqlTable targetTable = SqlTable.of(getTargetEndpoint().getTableId());

        try {
            MergeStatement query = ETLBuilder.getETLLoadMergeStatement(sourceTable, targetTable, jobTargetTableKeyFieldId);
            bqService.executeQuery(query.getSql());
            logger.atDebug().log("The table '{}' was merged with source data from {}",
            BqService.getQualifiedTable(getSourceEndpoint().getTableId()), BqService.getQualifiedTable(getTargetEndpoint().getTableId()));

        } catch (SqlBuilderException | BqException ex) {
            String msg = String.format("Table merge failed on table '%s'", getTargetEndpoint().getQualifiedTable());
            throw new PipelineException(msg, this, ex.getCause(), StatusCode.INTERNAL);
        }
    }

    void createStoreTable()
    {
        //From the step, determine the for the table to be created and the name of the table to be created.
        SqlTable schemaTable = getSourceEndpoint().getTable();
        TableId targetTableId = getTargetEndpoint().getTableId();

        if(Boolean.FALSE.equals(SqlTableDef.of(targetTableId).exists()))
        {

            try {
                //Get the create table sql
                CreateTableStatement query = schemaTable.getFields().getCreateTableStatement(targetTableId, true);
                bqService.executeQuery(query.getSql());
                logger.atDebug().log("Store table for for keyset '{}' was created in the database", this.getKeysetId());
            }
            catch (SqlBuilderException ex) {
                String msg = String.format("Cannot generate Sql for the creation of the store table to support the ETLLoad for keyset '%s' in the database.", this.getKeysetId());
                throw new PipelineException(msg, this, ex.getCause(), StatusCode.INVALID_ARGUMENT);
            }
            catch (BqException ex) {
                String msg = String.format("Error encountered while creating store table for keyset '%s' in the database.", this.getKeysetId());
                throw new PipelineException(msg, this, ex.getCause(), StatusCode.INVALID_ARGUMENT);
            }
        }
    }

    private void logOutEvents() {

        // Create a events list that will hold the CDC messages
        List<Event> messages =  new ArrayList<>();

        // Create the events to be stored in the stream
        Event event = new Event(getKeysetId(), getTimestamp());
        event.setUpdateType(getUpdateType());

        messages.add(event);

        // Add the ActionStream to the Results collection for the pipeline
        getResultEvents().get(QueueType.OUT).addAll(messages);
    }

    private void logErrorEvents() {

        //If we get here, something went wrong in the processing of the ETL pipeline
        //and we should simply requeue the action in the buffer
        for(Action action : actionBuffer)
        {
            getResultEvents().get(QueueType.ERROR).add(action.getEvent());
        }
    }

    protected static EtlPipeline of(Ingestion ingestion, Map<StepEndpointTemplate.EndpointType, PipelineEndpoint> pipelineEndpoints)
    {
        Marker marker = logger.logEntry("Constructor");
        logger.atDebug().log("ETLLoad.fromBuilder started");

        //Create the pipeline
        LoadPipeline pipeline = new LoadPipeline(ingestion, pipelineEndpoints);

        //Configure validation (but leave it disabled for now as we will enable it during the Run Method)
        pipeline.validator.addConditions(pipeline.getSourceEndpoint().validator, false);

        logger.logExit(marker);

        return pipeline;
    }
}
