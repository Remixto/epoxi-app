package io.epoxi.engine.etl.pipeline;

import io.epoxi.cloud.bq.BqException;
import io.epoxi.cloud.bq.BqService;
import io.epoxi.engine.etl.transform.StreamTransform;
import io.epoxi.engine.queue.action.Action;
import io.epoxi.util.sqlbuilder.ETLBuilder;
import io.epoxi.util.sqlbuilder.SqlBuilderException;
import io.epoxi.engine.Config;
import io.epoxi.repository.event.Event;
import io.epoxi.repository.event.EventKey;
import io.epoxi.repository.model.Ingestion;
import io.epoxi.repository.model.KeysetId;
import io.epoxi.repository.model.StepEndpointTemplate;
import io.epoxi.repository.model.StepType;
import io.epoxi.repository.model.Stream;
import io.epoxi.repository.model.MessageQueue.QueueType;
import io.epoxi.repository.event.ETLTimestamp;
import io.epoxi.cloud.logging.AppLog;
import io.epoxi.cloud.logging.StatusCode;
import io.epoxi.util.sqlbuilder.statement.CreateViewStatement;
import io.epoxi.util.sqlbuilder.statement.SelectStatement;
import io.epoxi.util.sqlbuilder.types.CodeField;
import io.epoxi.util.sqlbuilder.types.RowField;
import io.epoxi.util.sqlbuilder.types.SelectProto;
import io.epoxi.util.sqlbuilder.types.SqlField;
import io.epoxi.util.sqlbuilder.types.SqlFieldMap;
import io.epoxi.util.sqlbuilder.types.SqlTable;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;

import org.slf4j.Marker;

import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.StandardSQLTypeName;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TransformPipeline extends EtlPipeline {

    private final Integer etlLoadTableExpirationSeconds;
    private final Integer etlTransformViewExpirationSeconds;

    private TableId transformViewId;
    private final List<Stream> streams;

    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

    private TransformPipeline(Ingestion ingestion, Map<StepEndpointTemplate.EndpointType, PipelineEndpoint> pipelineEndpoints)
    {
        super(ingestion.getKeysetId());

        Marker marker = logger.logEntry("Constructor");

        KeysetId keysetId = ingestion.getKeysetId();
        bqService = new BqService(keysetId.getProjectName());

        //Configure the pipeline endpoints
        setEndpoints(pipelineEndpoints);

        //Configure other pipeline settings
        getActionBuffer().setMaxSize(Config.ACTION_BUFFER_DEFAULT_MAX_SIZE);
        setNumMaxErrors(3);
        etlLoadTableExpirationSeconds = Config.ETL_LOAD_TABLE_EXPIRATION_SECONDS;
        etlTransformViewExpirationSeconds = Config.ETL_TRANSFORM_VIEW_EXPIRATION_SECONDS;
        streams = ingestion.getStreams();

        logger.atDebug()
            .addKeyValue("Source", pipelineEndpoints.get(StepEndpointTemplate.EndpointType.SOURCE))
            .addKeyValue("Target", pipelineEndpoints.get(StepEndpointTemplate.EndpointType.TARGET))
            .log("TransformPipeline object initiated from builder");

        logger.logExit(marker);

        logger.atDebug().log("The pipeline was created for keyset '{}'", getKeysetId());

    }

    @Override
    public StepType getStepType() {

        return StepType.TRANSFORM;
    }

    /**
     * Run will be called after windowed messages are received.
     * At that time, if there is work to perform, cdc_Keys will contain items.
     */
    public void run() {

        Marker marker = logger.logEntry("run");
        logger.atDebug().log("ETLTransform.run started for '{}'", getKeysetId());

        //If there are no keys, there is no work to do.  As such, exit
        if (getActionBuffer().getMaxSize() == 0) return;

        // Get a list of the CDC message(s) to process and populate it into a new list of tempActions.
        //The tempActions list will be emptied as each of the actions are processed during OutputTransform.
        //If an exception occurs, the list will be repopulated with all the original ActionBuffer,
        //so that they may all be logged as errors
        List<Action> tempActions = new ArrayList<>(getActionBuffer());

        logger.atInfo().log("ETLTransform started for '{}'", this.getKeysetId());

        try {

                //This is the timestamp for the current action (it is new and based on now)
                if (getTimestamp()==null) setTimestamp(ETLTimestamp.now());

                //Use the timestamp to append to the name of the target
                getTargetEndpoint().setTimestamp(getTimestamp());

                //If the load table exists, drop it. This is typically not be the case as load tables are unique by Timestamp.
                //However, during debugging or upon unexpected code execution, it may exist.  As such, try to drop it.
                dropLoadTable();

                //Create the transform view
                createTransformView();

                // Execute the transform and output the results,
                // using the keys to filter the data transformed
                outputTransform(tempActions);

                // Validate the output that was generated
                validateOutput();

                // Log the results of the transform to a pub/sub queue for Merge to Store
                logOutEvents();

                logger.atInfo().log("ETLTransform completed for '{}'", this.getKeysetId());

        } catch (Exception ex) {

            incrementError();

            //Something happened.  As such, all they processed keys are now suspect
            //and should be added back to the tempKeys so that they can be logged into ErrorMessages
            tempActions = getActionBuffer();

            //Attempt to delete any etl_load table created
            dropLoadTable();

            //Rethrow the error
            String msg = String.format("%s Pipeline Error: %s", getStepType(), ex.getMessage());
            throw new PipelineException(msg, this, ex, StatusCode.INTERNAL);
        }
        finally
        {
            // If all went well, aryKeys will now be empty.
            // Any items left represent errors to be re-queued
            logErrorEvents(tempActions);
            getActionBuffer().clear();
        }

        logger.logExit(marker);

    }



    /**
     * Perform a transformation on all the actions passed to the method.
     * On error, a decreasing subset of the buffer is transformed until as many actions as possible are processed.
     * As a subset of actions are processed, they will removed from the actionBuffer();
     * @param actions The list of actions to transform
     */
    private void outputTransform(List<Action> actions) {

        logger.atDebug().log("ETLTransform.OutputTransform started for '{}'", getKeysetId());
        String query = ETLBuilder.getETLTransformTransformStatement(transformViewId, getSourceEndpoint().getKeyFieldId());

        //Get a (unique) list of all the keys to be processed
        Set<Long> etlLoadKeys = new HashSet<>();
        for (Action action : actions)
        {
            for ( EventKey eventKey : action.getEvent().getEventKeys())
            {
                etlLoadKeys.add(eventKey.getKey());
            }
        }
        int totalKeyCount = etlLoadKeys.size();

        //Attempt the transformation on all of the collected keys
        try
        {
            int position = etlLoadKeys.size();
            while (!etlLoadKeys.isEmpty() && getNumErrors() < getNumMaxErrors()) {

                try {
                    //Get a subset of the keys (initially it will be the entire set)
                    List<Long> keysToProcess = etlLoadKeys.stream().limit(position).collect(Collectors.toList());
                    tryOutputTransform(query, keysToProcess.toArray(new Long[position]));

                    // Keys were successfully transformed. Remove them from the set of keys
                    // remaining to be transformed.
                    etlLoadKeys.removeAll(keysToProcess);

                } catch (PipelineException ex) {
                    // Some data could not be exported. Retry with a small array of keys
                    incrementError();
                    position = position / 2;
                }
            }

            if (getNumErrors().equals(getNumMaxErrors())) {
                throw new PipelineException("Max errors exceeded", this, StatusCode.OUT_OF_RANGE);
            }
        }
        finally{
            //At the end, if there are keys left in etlLoadKeys, then
            //create a result action with all the keys and append it to the action list passed
            actions.clear();

            if (!etlLoadKeys.isEmpty())
            {
                Event event = new Event(this.getKeysetId());
                List<EventKey> keys = event.getEventKeys();
                for (Long key :etlLoadKeys)
                {
                    EventKey eventKey = new EventKey(key, EventKey.Type.REQUEUE);
                    keys.add(eventKey);
                }
                actions.add(new Action(StepType.TRANSFORM, event));
            }
        }

        //Log the outcome
        logger.atDebug()
            .addArg(totalKeyCount)
            .addArg(totalKeyCount - actions.size())
            .addArg(getNumErrors())
            .addArg(actions.size())
            .log("{} keys transformed. {} successful rows, {} unsuccessful rows and {} errors");
    }

    private void tryOutputTransform(String query, Long[] aryKeys) {

        Map<String, QueryParameterValue> parameters = new HashMap<>();
        parameters.put("CDC_keys", QueryParameterValue.array(aryKeys, StandardSQLTypeName.INT64));
        parameters.put("keyset_name", QueryParameterValue.string(getKeysetId().toString()));

        // Save the results of the above query to the destination table provided
        try {

            TableResult result = bqService.saveQueryResult(query, parameters, getTargetEndpoint().getTableId(), false, etlLoadTableExpirationSeconds);

            Long recordCount = result.getTotalRows();
            logger.atDebug().log("ETLTransform executed and output {} results to '{}''", recordCount, getTargetEndpoint().getQualifiedTable());

        } catch (BqException ex) {
            String msg = String.format("ETLTransform error during output to '%s'", getTargetEndpoint().getQualifiedTable());
            throw new PipelineException(msg, this, ex, StatusCode.INTERNAL);
        }
    }

    private void validateOutput(){

        logger.atDebug().log("ETLTransform.ValidateOutput started for '{}'", getKeysetId());

        //Declare an array for errors generated by validations.  Run through the validation steps and record failures.
        List<String> failMsg = new ArrayList<>();

        try {

            //VALIDATE: Duplicate keys
            SelectStatement query = ETLBuilder.getUniqueKeyCheck(getTargetEndpoint().getTableId(), getTargetEndpoint().getKeyFieldId());
            TableResult result = bqService.getQueryResult(query.getSql());

            long recordCount = result.getTotalRows();
            if (recordCount > 0)
            {
                failMsg.add("Pipeline failed. Non unique rows generated during OutputTransform.");
            }

            //Validation ended
            logger.atDebug().log("ETLTransform validation complete. {} errors found", failMsg.size());

        } catch (BqException ex) {
            String msg = String.format("Error encountered while Validating TransformOutput for '%s'", getTargetEndpoint().getQualifiedTable());
            throw new PipelineException(msg, this, ex, StatusCode.INTERNAL);
        }

        if (!failMsg.isEmpty())
        {
            String msg = String.join(", ", failMsg);
            throw new PipelineException(String.format("Validation of OutputTransform failed. %s", msg), this, StatusCode.INVALID_ARGUMENT);
        }
    }

    private void dropLoadTable()
    {
        logger.atDebug().log("ETLTransform.dropLoadTable started for '{}'", getKeysetId());

        String query = ETLBuilder.getDropTable(getTargetEndpoint().getTableId());

        try {
            bqService.getQueryResult(query);

            //Log the outcome
            logger.atDebug().log("The table '{}' was dropped", getTargetEndpoint().getTableId());

        } catch (BqException ex) {
            logger.atWarn().log("Attempted drop of table '{}' failed.", BqService.getQualifiedTable(getTargetEndpoint().getTableId()));
        }
    }

    private CreateViewStatement getTransformViewStatement()
    {
        logger.atDebug().log("ETLTransform.setTransformViewSchema started for '{}'", getKeysetId());

        //Set some basics for the source data (name and keyField)
        SqlTable sourceTable = SqlTable.of(getSourceEndpoint().getTableId());
        sourceTable.setAlias("e");

        transformViewId = TableId.of("temp", String.format("etl_transform_vw_%s", getKeysetId().getKeysetName()));

        //Build the select statement.  Either from protos, or as a simple select on the sourceTable
        SelectStatement select;
        if (streams.isEmpty())
        {
            SqlFieldMap<String, RowField> fields = sourceTable.getFields();
            fields.remove("row_hash");
            select = SelectStatement.select(fields).from(sourceTable);
        }
        else
        {
            //Try to assemble the SqlProtos that will comprise the view columns
            List<SelectProto> protos = new ArrayList<>();

            //Get the fields from the job transforms
            for(Stream stream : streams)
            {
                //Using a StreamTransform, convert the stream to a SqlProto
                try
                {
                    SelectProto proto = new StreamTransform(stream, sourceTable).getSelectProto();
                    protos.add(proto);
                }
                catch(IllegalArgumentException ex)
                {   String name = stream.getName();
                    if (name==null) name = stream.getSource().getName();
                    String msg = String.format("Could not create a SqlProto from the stream '%s' for keysetId '%s'.", name, getKeysetId());

                    throw new PipelineException(msg, this, ex, StatusCode.INVALID_ARGUMENT);
                }
            }
            select = SelectStatement.of(protos);
        }

        //Get the two extra fields for Date_Created and Date_Last_Modified
        CodeField dateCreated = new CodeField("CURRENT_DATETIME()", "Date_Created", StandardSQLTypeName.DATETIME);
        CodeField dateLastModified = new CodeField("CURRENT_DATETIME()", "Date_Last_Modified", StandardSQLTypeName.DATETIME);

        HashMap<String, SqlField> extraFields = SqlFieldMap.newBuilder()
                                                        .add(dateCreated)
                                                        .add(dateLastModified)
                                                        .build();

        select.getSelectFields().putAll(extraFields);

        //Log the outcome
        logger.atDebug().log("The schema for the Transform View was set for keyset '{}'", getKeysetId());

        return CreateViewStatement.createView(transformViewId, true, select);

    }

    /**
     * Creates a view statement in the database (overwriting if needed) to gather the data to be transformed during the pipeline
     */
    private void createTransformView()
    {
        logger.atInfo().log("ETLTransform.createTransformView started for '{}'", getKeysetId());

        CreateViewStatement transformViewStatement = getTransformViewStatement();

        try {
            bqService.executeQuery(transformViewStatement.getSql());

            //Set an expiration on the newly created view
            Integer expirationSeconds = etlTransformViewExpirationSeconds;
            if (expirationSeconds !=null)
                bqService.setTableExpirationDate(transformViewStatement.getViewTableId(), expirationSeconds);

            logger.atDebug().log("Transform view for keyset '{}' was created in the database", this.getKeysetId());

        }
        catch (SqlBuilderException ex) {
            String msg = String.format("Cannot generate Sql for the Transform view needed to support an ETLTransform of keyset '%s' in the database.", this.getKeysetId());
            throw new PipelineException(msg, this, ex.getCause(), StatusCode.INVALID_ARGUMENT);
        }
        catch (BqException ex) {
            String msg = String.format("Error encountered creating Transform view for keyset '%s' in the database.", this.getKeysetId());
            throw new PipelineException(msg, this, ex.getCause(), StatusCode.INVALID_ARGUMENT);
        }
    }

    private void logOutEvents() {

        // Create a list that will hold the CDC actions
        List<Event> events =  new ArrayList<>();

        // Create the action to be stored in the stream
        Event event = new Event(getKeysetId(), getTimestamp());
        event.setUpdateType(getUpdateType());
        events.add(event);

        // Add the Events to the Results collection for the pipeline
        getResultEvents().get(QueueType.OUT).addAll(events);

    }

    private void logErrorEvents(List<Action> actions) {

        //If there are no actions, return because there is nothing to log
        if (actions.isEmpty()) return;

        // Create a master action that contains all the keys in error
        List<EventKey> eventKeys = new ArrayList<>();
        for (Action action  : actions) {
            eventKeys.addAll(action.getEvent().getEventKeys());
        }

        Event masterEvent = new Event(getKeysetId(), getTimestamp());
        masterEvent.setEventKeys(eventKeys);

        // Create a list that will hold the error actions
        List<Event> errorEvents =  new ArrayList<>();
        errorEvents.add(masterEvent);

        // Add the ActionStream to the Results collection for the pipeline
        getResultEvents().get(QueueType.ERROR).addAll(errorEvents);
    }

    protected static EtlPipeline of(Ingestion ingestion, Map<StepEndpointTemplate.EndpointType, PipelineEndpoint> pipelineEndpoints)
    {
        TransformPipeline pipeline;
        pipeline = new TransformPipeline(ingestion, pipelineEndpoints);

        //Configure validation
        pipeline.validator.addAll(pipeline.getSourceEndpoint().validator);

        return pipeline;
    }
}
