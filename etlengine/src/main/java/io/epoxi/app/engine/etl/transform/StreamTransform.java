package io.epoxi.app.engine.etl.transform;

import io.epoxi.app.util.sqlbuilder.clause.SqlJoinClause;
import io.epoxi.app.cloud.logging.AppLog;
import io.epoxi.app.engine.Config;
import io.epoxi.app.repository.model.Stream;
import io.epoxi.app.util.sqlbuilder.types.RowField;
import io.epoxi.app.util.sqlbuilder.types.SelectProto;
import io.epoxi.app.util.sqlbuilder.types.SqlFieldMap;
import io.epoxi.app.util.sqlbuilder.types.SqlTable;
import lombok.Getter;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

public class StreamTransform {

    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

    @Getter
    /*
    * The stream to transform
     */
    private final Stream stream;

    @Getter
    /*
   * The table to which the stream will be joined, where the stream participates on the right hand side of a left join
    */
    private final SqlTable leftJoinTable;


    @Getter
    /*
    * The name of the in the source data that the lookup data will be joined to
     */
    private final String joinFieldId;

    @Getter
     /*
    * If true, the data retrieved from lookupTableDef will be returned as separate columns.
     * If false, it will be returned as a nested data (a STRUCT in BigQuery).
     * This value is true, when the stream used to create the transformation has a nestFieldId that is not null
     */
    private final Boolean nestFields;

    /***
     * Create a stream transformation. If the stream has a nestFieldId, the returned transform will be nested.
     * A join field will be defined by default, based on the following rules:
     * For the first stream proto created in a list of protos, it will not be used.
     * For subsequent protos the join will be created between the first proto
     * and the current proto based on the hashKeyFieldId of the current proto.
     * In Sql the current proto will be on the right side of a left join with the first proto
     *
     * @param stream  The SqlTableDef that defines the data to be retrieved
     * If false, it will be returned as a nested data (a STRUCT in BigQuery)

     */

    public StreamTransform (Stream stream, SqlTable leftJoinTable)
    {
        this(stream, leftJoinTable, stream.getHashKeyFieldId());
    }

    /***
     * Create a stream transformation.  If the stream has a nestFieldId, the returned transform will be nested.
     * @param stream  The SqlTableDef that defines the data to be retrieved
     * @param leftJoinTable The left hand table that the stream will be joined to
     * @param joinFieldId The name of the in the source data that the lookup data will be joined to
     */
    public StreamTransform (Stream stream, SqlTable leftJoinTable, String joinFieldId)
    {
        this.stream = stream;
        this.joinFieldId = joinFieldId;
        this.nestFields = (stream.getNestFieldId()!=null);
        this.leftJoinTable = leftJoinTable;
    }

    public SelectProto getSelectProto() {

        SelectProto proto;

        //Get the lookup fields from the stream
        SqlFieldMap<String, RowField> lookupFields = getStream().getSqlFieldMap();

        if (lookupFields == null || lookupFields.size() == 0)
        {
            logger.atWarn().addMessage("LookupFields are not set for '{}'. Cannot create a proto without a list of fields", getStream().getSource().getName());
            return null;
        }

        try {
            // Get the field list that corresponds to the short row for the table
            SqlFieldMap<String, RowField> fields = lookupFields;

            //Remove any reserved field names from the fieldList
            for (String fieldName : Config.RESERVED_FIELD_NAMES)
            {
                fields.remove(fieldName);
            }

            // If we are nestFields, then group the fields into a single rowField with
            // subfields
            if (Boolean.TRUE.equals(nestFields)) {

                //Pull the table object from the first subfield
                Object firstKey = fields.keySet().toArray()[0];
                SqlTable table = fields.get(firstKey).getTable();

                //Make a parent field and nest the lookupFields as subfields to the parent
                String parentFieldName = stream.getNestFieldId();

                List<RowField> subfields = new ArrayList<>(lookupFields.values());
                RowField parentField = RowField.ofNest(table, parentFieldName, parentFieldName, subfields);
                fields = new SqlFieldMap<>();
                fields.put(parentField.getName(), parentField);
            }

            //Assemble the proto
            RowField leftJoinField = leftJoinTable.getField(this.joinFieldId);
            RowField rightJoinField = lookupFields.get(getStream().getHashKeyFieldId());

            //If we didn't find a matching field on the right, try fetching the rightJoinField
            //by removing the nestFieldId prefix from the getHashKeyFieldId
            if (rightJoinField==null)
            {
                String rightFieldId = getStream().getHashKeyFieldId().replace(String.format("%s_", stream.getNestFieldId()), "");
                rightJoinField = lookupFields.get(rightFieldId);
            }
            SqlJoinClause join = SqlJoinClause.of(SqlJoinClause.JoinType.LEFT, leftJoinField, rightJoinField);
            proto = SelectProto.of(fields, join);



        }
        catch(IllegalArgumentException ex)
        {
            throw new IllegalArgumentException("A SelectProto cannot be created for ETLFKeyTransform. Check the cause for details.", ex);
        }

        return proto;
    }
}
