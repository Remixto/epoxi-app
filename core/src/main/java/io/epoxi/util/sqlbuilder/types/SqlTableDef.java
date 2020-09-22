package io.epoxi.util.sqlbuilder.types;

import com.google.cloud.bigquery.TableId;

import com.google.gson.FieldNamingStrategy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import io.epoxi.cloud.bq.BqService;

import java.lang.reflect.Field;


public class SqlTableDef {

    @Expose String tableId;
    @Expose String keyFieldId;
    private String alias;


    protected SqlTableDef( TableId tableId)
    {
        this(tableId, null) ;
    }

    protected SqlTableDef( TableId tableId, String keyFieldId)
    {
        if (tableId ==null)
        {
            throw new IllegalArgumentException();
        }

        setTableId(tableId);
        this.keyFieldId = keyFieldId;
    }

    public SqlTable getTable() {

        return SqlTable.of(getTableId(), alias);
    }

    public Boolean exists() {

        BqService bq = new BqService();
        return (bq.getTable(getTableId()) !=null);
    }

    public String toJson() {

		GsonBuilder builder = new GsonBuilder();
		builder.setPrettyPrinting().serializeNulls();
		builder.setFieldNamingStrategy(new ClassFieldNamingStrategy());
		Gson gson = builder.create();
		return gson.toJson(this);
    }

    // Custom FieldNamingStrategy
    private static class ClassFieldNamingStrategy implements FieldNamingStrategy {
        @Override
       public String translateName(Field field) {
           return field.getName();
       }
    }

    public TableId getTableId() {

        return BqService.getTableId(tableId.split("\\."));
    }

    private void setTableId(TableId value) {

        tableId = BqService.getQualifiedTable(value);
    }

    public String getQualifiedTableId() {

        return tableId;
    }

    public String getKeyFieldId() {
        return keyFieldId;
    }

    public void setKeyFieldId(String keyFieldId) {
        this.keyFieldId = keyFieldId;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public static SqlTableDef of(TableId tableId)
    {
        return new SqlTableDef(tableId);
    }

    public static SqlTableDef of(TableId tableId, String keyFieldId)
    {
        return new SqlTableDef(tableId, keyFieldId);
    }
}
