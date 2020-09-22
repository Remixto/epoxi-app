package io.epoxi.util.sqlbuilder;

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;

import io.epoxi.util.sqlbuilder.statement.CreateTableStatement;
import io.epoxi.util.sqlbuilder.statement.MergeStatement;
import io.epoxi.util.sqlbuilder.statement.SelectStatement;
import io.epoxi.cloud.bq.BqService;

import io.epoxi.util.sqlbuilder.types.*;

public class ETLBuilder {

    private ETLBuilder()
    {

    }

    /**
     * Gets a merge statement to be performed during ETLLoad pipeline
     * @param tSource the source data table
     * @param tTarget the target data table to be merged into
     * @return An MERGE statement, such as :
        MERGE store.Entity t
        USING ( SELECT  *,
                        CURRENT_DATETIME() as Date_Created,
                        CURRENT_DATETIME() as Date_Last_Modified
                FROM    etl_load.Firm_Data_Entity d
            ) s
        ON    t.Entity_Key = s.Entity_Key

        WHEN MATCHED THEN
        UPDATE SET
        -- Is_Deleted = s.Is_Deleted,
            Entity_Code = s.Entity_Code,
            Entity_Name = s.Entity_Name,
            Legal_Name = s.Legal_Name

        WHEN NOT MATCHED BY TARGET THEN
        INSERT (Entity_Code, Entity_Name)
        VALUES (Entity_Code, Entity_Name)

        WHEN NOT MATCHED BY SOURCE THEN  --WHEN etl_type_code = 'Full'
        UPDATE SET
        Is_Deleted = true,
        Date_Last_Modified = CURRENT_DATETIME();
     */
    public static MergeStatement getETLLoadMergeStatement(SqlTable tSource, SqlTable tTarget, String keyFieldId)
    {
        //Set an alias for tables involved in the de-normalization to make the resulting query more easy to read
        if (tSource.getAlias().equals(tSource.getName())) tSource.setAlias("s");
        if (tTarget.getAlias().equals(tTarget.getName())) tTarget.setAlias("t");

        //Add two calculated fields to the source data
        CodeField dateCreated = new CodeField(CURRENT_DATETIME, DATE_CREATED, StandardSQLTypeName.DATETIME);
        CodeField dateLastModified = new CodeField(CURRENT_DATETIME, DATE_LAST_MODIFIED, StandardSQLTypeName.DATETIME);

        SqlFieldMap<String, SqlField> extraFields = new SqlFieldMap<>();
        extraFields.put(dateCreated.getName(), dateCreated);
        extraFields.put(dateLastModified.getName(), dateLastModified);

        //Get the merge statement
        return MergeStatement
        .merge(tTarget)
        .using(tSource, extraFields, SqlPredicate.newBuilder()
                                        .setFieldPair(new SqlFieldPair(tSource, tTarget, keyFieldId))
                                        .build()
                )
        .whenMatched()
        .whenNotMatchedByTarget()
        .whenNotMatchedBySource( SqlFieldPair.newBuilder()
                                            .setField1 (tTarget.getField("Is_Deleted"))
                                            .setField2( new LiteralField(true))
                                            .build(),

                                            SqlFieldPair.newBuilder()
                                                .setField1 (tTarget.getField(DATE_LAST_MODIFIED))
                                                .setField2( new CodeField(CURRENT_DATETIME))
                                                .build()
                                );



    }

    public static SelectStatement getUniqueKeyCheck(TableId tableId, String keyFieldId)
    {
        SqlTable tTable = SqlTable.of(tableId, "e");

        assert tTable != null;
        RowField keyField = tTable.getField(keyFieldId);

        return SelectStatement
            .select( keyField)
            .from(tTable)
            .groupBy(keyField)
            .code(String.format("HAVING count(%s) > 1", keyField.getPrefixPath()));
    }

    public static CreateTableStatement getCreateExtractTable()
    {
       return new CreateTableStatement();

    }

    public static CreateTableStatement getCreateStoreTable(TableId storeTableId, TableId transformViewId)
    {
        SqlTable tTable = SqlTable.of(transformViewId, "s");
        assert tTable != null;
        SqlFieldMap<String, RowField> fieldMap = tTable.getFields();

        CodeField dateCreated = new CodeField(CURRENT_DATETIME, DATE_CREATED, StandardSQLTypeName.DATETIME);
        CodeField dateLastModified = new CodeField(CURRENT_DATETIME, DATE_LAST_MODIFIED, StandardSQLTypeName.DATETIME);

        SqlFieldMap<String, SqlField> fields = SqlFieldMap.newBuilder().add(fieldMap).add(dateCreated).add(dateLastModified).build();

        return CreateTableStatement.createTable(storeTableId, true, fields);

    }

    public static String getETLExtractGenerateCDCStatement()
    {
        return "CALL etl_engine.sp_Extract_CDC_Update ( @table_Id, @keyset_name, @keyfield_Id, @etl_update_type, @cdc_timestamp, @delete_threshold);";
    }

    public static String getETLExtractGenerateCDCResultStatement()
    {
        String newLine = "\n";
        return String.join(    newLine,
                                        "SELECT  Key, keyset_name, Last_CDC_Action, Last_Timestamp",
                                        "FROM    etl_engine.vw_Extract_CDC",
                                        "WHERE   Keyset_ID = etl_engine.fn_Keyset_ID_From_Name(@keyset_name)",
                                        "AND     Last_Timestamp = @cdc_timestamp;");

    }

    public static String getETLTransformTransformStatement(TableId sourceId, String keyFieldId)
    {
        // Build the query
        String newLine = "\n";
        return  String.join( newLine,
                                     "SELECT CASE WHEN cdc.Last_CDC_Action = 'D' THEN true ELSE false END as Is_Deleted,",
                                     "       e.*",
                                     String.format("FROM    `%s` e", BqService.getQualifiedTable(sourceId)),
                                     String.format("            INNER JOIN etl_engine.vw_Extract_CDC cdc on e.%s = cdc.Key", keyFieldId),
                                     "            CROSS JOIN UNNEST(@CDC_keys) as cdc_Key on cdc.Key = cdc_Key",
                                     "WHERE   cdc.Keyset_ID = etl_engine.fn_Keyset_ID_From_Name(@keyset_name);");



    }



    public static String getDropTable(TableId tableId)
    {
        return String.format("DROP TABLE IF EXISTS `%s`;", BqService.getQualifiedTable(tableId));
    }

    private static final String DATE_LAST_MODIFIED = "Date_Last_Modified";
    private static final String DATE_CREATED = "Date_Created";
    private static final String CURRENT_DATETIME = "CURRENT_DATETIME()";

}
