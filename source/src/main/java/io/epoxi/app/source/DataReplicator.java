package io.epoxi.app.source;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.TableId;
import io.epoxi.app.cloud.bq.BqService;
import io.epoxi.app.repository.model.SourceType;
import io.epoxi.app.repository.model.Stream;
import io.epoxi.app.source.reader.SourceReader;
import io.epoxi.app.source.reader.connection.Connection;
import io.epoxi.app.source.reader.connection.BqConnection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.values.PCollection;

public class DataReplicator {

    private final SourceReader reader;
    Options options;   

    private DataReplicator(Options options, SourceReader reader) {
        this.options = options;
        this.reader = reader;
    }

    public void run()
    {
        //Delete any old target table from a previous run
        TableId targetTableId = BqService.getTableId(options.getOutput());
        BqService bq = new BqService();
        bq.deleteTable(targetTableId);

        //Create the pipeline and apply a rootPCollection on it
        Pipeline pipeline = Pipeline.create(options);
        PCollection<TableRow> rootCollection = reader.getReader(pipeline);  

        //Write the data to the output table

        String outputTable = String.format("%s.%s", targetTableId.getDataset(), targetTableId.getTable());
        rootCollection.apply(
            BigQueryIO.writeTableRows()
                .to(outputTable)               
                .withSchema(options.getOutputSchema())
                .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)                
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));               
        pipeline
                .run()
                .waitUntilFinish();

        //Set an expiration on the newly created table
        Integer expirationSeconds = options.getOutputTableExpirationSeconds();
        if (expirationSeconds !=null)
            bq.setTableExpirationDate(targetTableId, expirationSeconds);
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }
    
    public static class Builder
    {
        Stream stream;
        TableId targetTable;
        String pipelineTempDirectory;
        Integer tableExpirationSeconds = null;  //Does not expire

        public Builder setStream(Stream stream)
        {
            this.stream = stream;
            return this;
        }

        public Builder setPipelineTempDirectory(String pipelineTempDirectory)
        {
            this.pipelineTempDirectory = pipelineTempDirectory;
            return this;
        }



        public Builder setTargetTable(TableId targetTable)
        {
            this.targetTable = targetTable;
            return this;
        }

        public Builder setTargetTableExpiration(Integer tableExpirationSeconds) {
            this.tableExpirationSeconds = tableExpirationSeconds;
			return this;
		}

        public DataReplicator build()
        {   
            //Set the reader
            SourceReader reader = getReader();
            Options options = getOptions();

            //Create and return the replicator
            return new DataReplicator(options, reader);
        }

        private SourceReader getReader()
        {        
            // Get the connection
            Connection connection = getTabularConnection(stream);
            return connection.getReader();
        }
        
        private Options getOptions()
        {        
            //Set the options for the replication            
            GcpOptions gcpOptions = PipelineOptionsFactory.create().as(GcpOptions.class);
            gcpOptions.setProject(targetTable.getProject());       
            
            //gcpOptions.setGcpCredential(getCredentials());  //TODO make this work and allow the setting of credentials for any source
            
            Options replicatorOptions = gcpOptions.as(Options.class);
            replicatorOptions.setBigQueryLoadingTemporaryDirectory(StaticValueProvider.of(pipelineTempDirectory));
            replicatorOptions.setOutput(BqService.getQualifiedTable(targetTable));         
            replicatorOptions.setOutputSchema(stream.getSqlFieldMap().toTableSchema());
            replicatorOptions.setOutputTableExpirationSeconds(tableExpirationSeconds);

            return replicatorOptions;
        }
        
        private Connection getTabularConnection(Stream stream)
        {
            SourceType sourceType = stream.getSource().getSourceType();
    
            switch (sourceType.getName())
            {
                case "BQ":
                    return BqConnection.of(stream);
                default:
                    throw new UnsupportedOperationException(String.format("The Source Type '%s' does not exist or has not yet implemented", sourceType.getName()));  //TODO add more connect types
            }
        }
        
        private Credentials getCredentials() {
            return NoCredentials.getInstance();  //TODO figure out how to get credentials from the properties of the source
        }
    }

    /**
       * Options supported by bqConnector.
       *
       * <p>Inherits standard configuration options.
       */
      public interface Options extends PipelineOptions {   
        @Description(
            "BigQuery table to write to, specified as "
                + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
        @Validation.Required
        String getOutput();
    
        void setOutput(String value);
    
        @Description(
            "BigQuery schema of the table to write to")
        @Validation.Required
        TableSchema getOutputSchema();
    
        void setOutputSchema(TableSchema value);
    
        @Description(
            "BigQuery expiration (seconds) of the table to write to")
        @Validation.Required
        Integer getOutputTableExpirationSeconds();
    
        void setOutputTableExpirationSeconds(Integer value);

        @Validation.Required
        @Description("Temporary directory for BigQuery loading process")
        ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

        void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);
    }   
}
