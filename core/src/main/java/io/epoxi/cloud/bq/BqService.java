package io.epoxi.cloud.bq;

import com.google.auth.Credentials;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.BigQuery.JobField;
import com.google.cloud.bigquery.BigQuery.JobOption;
import com.google.cloud.bigquery.QueryJobConfiguration.Builder;
import io.epoxi.cloud.logging.AppLog;
import io.epoxi.cloud.logging.StatusCode;
import org.threeten.bp.Duration;

import java.lang.invoke.MethodHandles;
import java.time.OffsetDateTime;
import java.util.Map;

/**
 * Helper classes for Bigquery.
 */
public final class BqService {

    BigQuery bq;
    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

    public BqService() {
        bq = getBigQuery(null);
    }

    public BqService(String projectId) {
        bq = getBigQuery(projectId);
    }

    public BqService(String projectId, Credentials credentials) {
        bq = getBigQuery(projectId, credentials);
    }

    /**
     * Validates that the query will run without error and throws an error containing information on the cause if it is invalid.
     * @param query The query to be validated
     * @throws BqException Thrown when a BigQuery exception occurs
     */
    public void validateQuery(String query)
    {
        getQueryResult(query, null, true);
    }

    public void validateQuery(String query, Map<String, QueryParameterValue> parameters)
    {
        getQueryResult(query, parameters, true);
    }

    public void executeQuery(String query)
    {
        getQueryResult(query, null, false);
    }

    public void executeQuery(String query, Map<String, QueryParameterValue> parameters)
    {
        getQueryResult(query, parameters, false);
    }

    public TableResult getQueryResult(String query)
    {
        return getQueryResult(query, null, false);
    }

    public TableResult getQueryResult(String query, Map<String, QueryParameterValue> parameters)
    {
        return getQueryResult(query, parameters, false);
    }

    private TableResult getQueryResult(String query, Map<String, QueryParameterValue> parameters, Boolean dryRun)
    {
        Builder builder = QueryJobConfiguration.newBuilder(query)
                                .setDryRun(dryRun);
        if(parameters != null) builder.setNamedParameters(parameters);
        QueryJobConfiguration queryConfig = builder.build();

        // Get the result
        try {
            return bq.query(queryConfig);
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new BqException("Error getting query result", query, ex, StatusCode.CANCELLED);
        }
        catch ( BigQueryException ex) {

            // Handle exception
            throw new BqException("BigQuery Exception during query execution", query, parameters, ex, StatusCode.INTERNAL);
            }
        catch ( JobException ex) {
            // Handle exception
            throw new BqException("Job Exception during query execution", query, parameters,ex, StatusCode.ABORTED);
            }
        catch (Exception ex) {
            // Handle exception
            throw new BqException("Unexpected error during query execution", query, parameters, ex, StatusCode.UNKNOWN);
            }
    }

    public TableResult saveQueryResult(TableId viewId, TableId destinationTableId, Boolean overwriteDestination)
    {
        String query = "SELECT * FROM " + viewId.getProject() + "." + viewId.getDataset() + "." + viewId.getTable() + ";";

        return saveQueryResult(query, destinationTableId, overwriteDestination);
    }

    public TableResult saveQueryResult(String query, TableId destinationTableId, Boolean overwriteDestination)
    {
        return saveQueryResult(query, destinationTableId, overwriteDestination, null);
    }

    public TableResult saveQueryResult(String query, TableId destinationTableId, Boolean overwriteDestination, Integer expirationSeconds)
    {
        return saveQueryResult(query, null, destinationTableId, overwriteDestination, expirationSeconds);
    }

    public TableResult saveQueryResult(String query, Map<String, QueryParameterValue> parameters, TableId destinationTableId, Boolean overwriteDestination, Integer expirationSeconds)
    {
        QueryJobConfiguration queryConfig = getQueryConfig(query, parameters, destinationTableId, false);
        TableResult result = saveQueryResult(queryConfig, overwriteDestination);
        setTableExpirationDate(destinationTableId, expirationSeconds);
        return result;
    }

    private TableResult saveQueryResult(QueryJobConfiguration queryConfig, Boolean overwriteDestination)
    {
        //Delete the table if it exists
        if (Boolean.TRUE.equals(overwriteDestination)) deleteTable(queryConfig.getDestinationTable());

        // Get the result
        try {
            return bq.query(queryConfig);
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new BqException("Error saving query result", queryConfig.getQuery(), queryConfig.getNamedParameters(), ex, StatusCode.CANCELLED);
        }
        catch ( BigQueryException ex) {
            // Handle exception
            throw new BqException("BigQuery Exception while saving query result", queryConfig.getQuery(), queryConfig.getNamedParameters(), ex, StatusCode.INTERNAL);
            }
        catch ( JobException ex) {
            // Handle exception
            throw new BqException("Job Exception while saving query result", queryConfig.getQuery(), queryConfig.getNamedParameters(),ex, StatusCode.ABORTED);
            }
        catch (Exception ex) {
            // Handle exception
            throw new BqException("Unexpected error saving query result", queryConfig.getQuery(), queryConfig.getNamedParameters(), ex, StatusCode.UNKNOWN);
        }
    }



    public Schema getTableSchema(String query, Map<String, QueryParameterValue> parameters)
    {
        QueryJobConfiguration queryConfig = getQueryConfig(query, parameters, null, true);
        try {

            TableResult tableResult = bq.query(queryConfig);
            return tableResult.getSchema();
          }
          catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new BqException("Execution Interrupted", ex, StatusCode.CANCELLED);
          }
    }


    /**********************************************
     * Static Methods
     **********************************************/

    public void setTableExpirationDate(TableId tableId, Integer expirationSeconds)
    {
        Table table = getTable(tableId);
        if (expirationSeconds > 0)
        {
            long newExpiration = OffsetDateTime.now().plusSeconds(expirationSeconds).toInstant().toEpochMilli();
            table.toBuilder().setExpirationTime(newExpiration).build().update();
        }
    }

    public Table getTable(String datasetName, String tableName) {
        TableId tableId = TableId.of(datasetName, tableName);
        return getTable(tableId);
    }

    public Table getTable(String projectId, String datasetName, String tableName) {
        TableId tableId = TableId.of(projectId, datasetName, tableName);
        return getTable(tableId);
      }

    public Table getTable(TableId tableId) {


        return bq.getTable(tableId);
    }

    public void createTable(TableId tableId, Schema schema, Boolean overwrite)
    {
        createTable(tableId, schema, null, overwrite);
    }

    public void createTable(TableId tableId, Schema schema, TimePartitioning timePartitioning, Boolean overwrite)
    {
        try
        {
            //Delete the table if it already exists
            if (Boolean.TRUE.equals(overwrite) && getTable(tableId)!=null ) deleteTable(tableId);

            //Create the new TableInfo
            TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                    .setSchema(schema)
                    .setTimePartitioning(timePartitioning)
                    .build();

            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

            //Create the table
            bq.create(tableInfo);
        }
        catch(Exception ex)
        {
            String msg = String.format("Error creating table '%s'", getQualifiedTable(tableId));
            throw new BqException(msg, ex, StatusCode.INTERNAL);
        }
    }

    public void copyTable(TableId sourceTableId, TableId destinationTableId, Boolean overwrite)
    {
        JobOption options = JobOption.fields(JobField.STATUS, JobField.USER_EMAIL);

        if (Boolean.TRUE.equals(overwrite)) deleteTable(destinationTableId);

        Table sourceTable = getTable(sourceTableId);
        Job job = sourceTable.copy(destinationTableId, options);

        // Wait for the job to complete.
        try
        {
            Job completedJob =
                job.waitFor(
                    RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
                    RetryOption.totalTimeout(Duration.ofMinutes(3)));
            if (completedJob != null && completedJob.getStatus().getError() == null) {
                // Job completed successfully.
                logger.atDebug().log("Table was not copied");
            } else {

                String msg = "";
                if (completedJob!=null) msg = completedJob.getStatus().getError().getMessage();

                // Handle error case.
                logger.atWarn().addKeyValue("result", msg).log("Table was not copied");

            }
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            String msg = String.format("Error copying table '%s' to '%s'", getQualifiedTable(sourceTableId), getQualifiedTable(destinationTableId));
            throw new BqException(msg, ex, StatusCode.CANCELLED);
        }
        catch (Exception ex) {
            String msg = String.format("Error copying table '%s' to '%s'", getQualifiedTable(sourceTableId), getQualifiedTable(destinationTableId));
            throw new BqException(msg, ex, StatusCode.UNKNOWN);
        }
    }

    public void deleteTable(TableId tableId) {
        Table table = getTable(tableId);

        try{

            if (table != null)
                table.delete();
        }
        catch (Exception ex) {

            String msg = String.format("Error deleting table %s", getQualifiedTable(tableId));
            throw new BqException(msg, ex, StatusCode.UNKNOWN);
        }
    }

    public static String getQualifiedTable(TableId tableId)
    {
        if (tableId.getProject() == null)
        {
            return tableId.getDataset() + '.' + tableId.getTable();
        }
        else
        {
            return tableId.getProject() + '.' + tableId.getDataset() + '.' + tableId.getTable();
        }
    }

    public static TableId getTableId(String[] strings) {


        if (strings.length ==3)
        {
            return TableId.of(strings[0], strings[1], strings[2]);
        }
        else
        {
            return TableId.of(strings[0], strings[1]);
        }
    }

    /**
   * Get the tableID from a single string delimited with dots.
   * @param tableId the tableId as a string in the form [project].dataset.tableName
   * @return A BigQuery TableId object
   */
    public static TableId getTableId(String tableId) {

        String[] strings = tableId.split("\\.");
        return getTableId(strings);
    }

    private QueryJobConfiguration getQueryConfig(String query, Map<String, QueryParameterValue> parameters, TableId destinationTableId, Boolean dryRun)
    {
        Builder b = QueryJobConfiguration.newBuilder(query);
        b.setDryRun(dryRun);
        if(destinationTableId !=null) b.setDestinationTable(destinationTableId);
        if(parameters !=null) b.setNamedParameters(parameters);

        return b.build();
    }

    private static BigQuery getBigQuery(String projectId)
    {
        return getBigQuery(projectId, null);
    }

    private static BigQuery getBigQuery(String projectId, Credentials credentials)
    {
        BigQuery bigQuery;
        if (credentials ==null) credentials = BigQueryOptions.getDefaultInstance().getCredentials();

        if (projectId !=null)
        {
            bigQuery = BigQueryOptions.newBuilder()
                .setProjectId(projectId)
                .setCredentials(credentials)
                .build()
                .getService();
        }
        else
        {
            bigQuery = BigQueryOptions.getDefaultInstance().getService();
        }

        return bigQuery;

    }

}
