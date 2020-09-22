package io.epoxi.util.sqlbuilder.types;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.TableId;
import io.epoxi.util.sqlbuilder.statement.CreateTableStatement;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

public class SqlFieldMap<K, V extends SqlField> extends LinkedHashMap<K, V> {

    private static final long serialVersionUID = -906075021871824190L;

    public SqlFieldMap() {
    }

    @Override
    public V put(K key, V value) {
        super.put(key, value);
        return value;
    }

    public TableSchema toTableSchema()
    {
        List<TableFieldSchema> schemaFields = new ArrayList<>();

        this.values().forEach(f -> schemaFields.add(new TableFieldSchema().setName(f.getName()).setType(f.getType().toString())));

        return new TableSchema().setFields(schemaFields);

    }

    public static SqlFieldMap<String, RowField> of(SqlTable table, List<Field> fields) {

        SqlFieldMap<String, RowField> map = new SqlFieldMap<>();

        for (Field field : fields) {
            map.put(field.getName(), RowField.of(table, field));
        }
        return map;
    }

    public static SqlFieldMap<String, SqlField> of(List<? extends SqlField> fields) {

        SqlField[] fieldArray = fields.toArray(new SqlField[fields.size()]);
        return of(fieldArray);
    }

    public static SqlFieldMap<String, SqlField> of(SqlField[] fields) {

        SqlFieldMap<String, SqlField> map = new SqlFieldMap<>();

        for (SqlField field : fields) {
            map.put(field.getName(), field);
        }

        return map;
    }

    @SuppressWarnings("unchecked")
    public CreateTableStatement getCreateTableStatement(TableId tableId, Boolean allowReplace)
    {
        SqlFieldMap<String, SqlField> map = (SqlFieldMap<String, SqlField>)this;
        return CreateTableStatement.createTable(tableId, allowReplace, map);
    }



    public static FieldMapBuilder newBuilder()
    {
        return new FieldMapBuilder();
    }


    /*******************************************
     * SelectClauseBuilder
     *****************************************/

    public static class FieldMapBuilder{

        final SqlFieldMap<String, SqlField> map = new SqlFieldMap<>();

        public FieldMapBuilder add(SqlFieldMap<String, ? extends SqlField> sqlFieldMap)
        {
            for(Entry<String, ? extends SqlField> entry : sqlFieldMap.entrySet())
            {
                SqlField field = entry.getValue();
                map.put(field.getName(), field);
            }

            return this;
        }

        public FieldMapBuilder add(SqlTable table, FieldList fieldList)
        {
            map.putAll(SqlFieldMap.of(table, fieldList));

            return this;
        }

        public FieldMapBuilder add(SqlField... expressions)
        {
            for(SqlField field : expressions)
            {
                map.put(field.getName(), field);
            }

            return this;
        }

        public FieldMapBuilder add(List<SqlField> fieldList)
        {
            for(SqlField field : fieldList)
            {
                map.put(field.getName(), field);
            }

            return this;
        }

        public SqlFieldMap<String, SqlField> build()
        {
            return map;
        }
     }

}
