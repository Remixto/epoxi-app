package io.epoxi.app.engine.etl.pipeline;

import io.epoxi.app.util.validation.Validator;
import io.epoxi.app.cloud.bq.BqService;
import io.epoxi.app.repository.event.ETLTimestamp;
import io.epoxi.app.cloud.logging.AppLog;
import io.epoxi.app.util.sqlbuilder.types.SqlTable;
import io.epoxi.app.util.validation.ValidationCondition;

import com.google.cloud.bigquery.TableId;
import org.slf4j.Marker;
import lombok.Getter;
import lombok.Setter;

import java.lang.invoke.MethodHandles;
import java.util.Optional;

public class PipelineEndpoint {

    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

    private final String tableId;
    private final String keyFieldId;

    Validator validator = new Validator();

    //If a timestamp is set, it will be used, the Long value of the timestamp will be appended to the name of the TableId of the endpoint
    @Getter @Setter
    private ETLTimestamp timestamp;

    public PipelineEndpoint (TableId tableId, String keyFieldId)
    {
        this(BqService.getQualifiedTable(tableId), keyFieldId);
    }

    private PipelineEndpoint (String tableId, String keyFieldId)
    {
        Marker marker = logger.logEntry("Constructor");

        this.tableId = tableId;
        this.keyFieldId = keyFieldId;

        validator.add(new EndpointNotExistsCondition(this));
        logger.logExit(marker);
    }

    public String getKeyFieldId()
    {
        if (keyFieldId == null) return null;

        //If the field contains substitution values, return the string as it is
        if (keyFieldId.contains("{")) return keyFieldId;

        //If we are here, we have a not null string without substitutions.
        //The field name may have been passed prefixed with tableId or projectName.
        //If so, ignore these and pass only the final part
        String[] strings = keyFieldId.split("\\.");
        return strings[strings.length-1];
    }

    /**
     * Get the tableID of the endpoint.  If a timestamp has been set for the endpoint,
     * the tableId returned will contain a suffix that contains a Long representation of the timestamp
     */
    public TableId getTableId() {

        String[] strings = tableId.split("\\.");

        //Append the timestamp as a Long to the end of the tableName
        if (timestamp !=null)
            strings[strings.length-1] = String.format("%s_%s", strings[strings.length-1], timestamp.toNanos());

        return BqService.getTableId(strings);
    }

    public String getQualifiedTable()
    {
        return BqService.getQualifiedTable(getTableId());
    }

    public SqlTable getTable()
    {
        return SqlTable.of(getTableId());
    }

    public String toString()
    {
        return getQualifiedTable();
    }

    public static class EndpointNotExistsCondition implements ValidationCondition {

        final PipelineEndpoint endpoint;

        public EndpointNotExistsCondition(PipelineEndpoint endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public Optional<String> validate() {

            if (endpoint.getTable() != null)
                return Optional.empty();
            else
                return Optional.of(String.format("The table specified in the endpoint does not exist '%s'", endpoint.toString()));
        }
    }

     /**
     * Throws InvalidStateException if the validation fails
     */
	public void validOrThrow() {
        validator.validOrThrow(this.getQualifiedTable());
	}
}
