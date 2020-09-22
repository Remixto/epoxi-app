package io.epoxi.app.util.sqlbuilder.types;

import com.google.cloud.bigquery.*;
import io.epoxi.app.cloud.bq.BqService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class SqlTable {

    private final  TableId tableId;
    private String alias;
    private final FieldList fields;

    private SqlTable(Table table, String alias)
    {
       if (table==null) throw new IllegalArgumentException("TableId must be a table that exists");

       this.tableId = table.getTableId();
       fields = Objects.requireNonNull(table.getDefinition().getSchema()).getFields();
       if (alias!=null && !alias.equals(getName())) this.alias = alias;  //Don't set the alias if it is the same as the table name
    }

    public SqlFieldMap<String, RowField> getFields()
    {
        return SqlFieldMap.of(this, fields);
    }

    public SqlFieldMap<String, RowField> getFields(List<String> fieldNames)
    {
        return getFields(fieldNames.toArray(new String[fieldNames.size()]));
    }

    public SqlFieldMap<String, RowField> getFields(String... fieldNames)
    {
        SqlFieldMap<String, RowField> filteredList = new SqlFieldMap<>();

        for(String name : fieldNames)
        {
            RowField field = getField(name);
            if (field !=null)
            {
                filteredList.put(name, getField(name));
            }
        }

        return filteredList;

    }

    public RowField getField(String fieldName)
    {
        RowField field = null;

        try
        {
            field = getField(fields, fieldName, null);
        }
        catch(Exception ex)
        {
            //Do nothing.  The field was not found
            //TODO put a logger warning here.
        }

        return field;
    }

    private RowField getField(FieldList fieldList, String fieldName, RowField parent)
    {
        RowField rowField = null;
        try
        {
            //Split the name into parts so we can traverse it in stages.
            List<String> strings = new ArrayList<>(Arrays.asList(fieldName.split("\\.")));

            //Find and set the foundRow based on the first String found in the array of fieldName parts
            Field field = fieldList.get(strings.get(0));
            if (field == null) return null;
            RowField foundRow = RowField.of(this, field);

            if (strings.size() == 1)
            {
                //Only one string, then we have found the final RowField.
                //As such, we can assign it and skip to end of the method.
                rowField = RowField.of(this, field, null, parent);
            }
            else
            {
                //There are child subFields still to traverse until we find the final RowField.
                //Keep digging down, passing foundRow as the parent
                strings.remove(0);
                String newFieldName = String.join(".", strings);
                rowField = getField(field.getSubFields(), newFieldName, foundRow);
            }
        }
        catch(Exception ex)
        {
            //Do nothing.  The field was not found
            //TODO put a logger warning here.
            System.out.println(ex.getMessage());
        }

        return rowField;
    }

    /**
     * Get a field from the SqlTable that matches the RowField specified
     * @param field The RowField that will to be used to find the matching field in the SqlTable
     * @return If a match is found, the matching field in the table is returned.
     * If no match is found, the method returns null.
     */
    public RowField getField(RowField field)
    {
        RowField matchingField;
        try
        {
            matchingField =  RowField.of(this, fields.get(field.getName()));
            if (matchingField.equals(field))
            {
                return matchingField;
            }

        }
        catch(Exception ex)
        {
            //Do nothing.  The field was not found
            //TODO put a logger warning here.
        }
        return null;
    }

    public SqlFieldMap<String, RowField> getFields(StandardSQLTypeName type){

        SqlFieldMap<String, RowField> filteredList = new SqlFieldMap<>();

        for(Field field : fields)
        {
            StandardSQLTypeName sqlType = field.getType().getStandardType();
            if(sqlType.equals(type))
            {
                filteredList.put(field.getName(), RowField.of(this, field));
            }
        }
        return filteredList;
    }

    /**
     * Looks through the table and matches the fields with table passed to the method.
     * For each match found, a pair is created and returned as part of a list.
     * @param fieldsToMatch The list of fields that will be compared against to return matches
     * @return A paired field list.
     */
    public SqlFieldPairList getFieldPairs (Iterable<? extends SqlField> fieldsToMatch)
    {
        SqlFieldPairList pairs = new SqlFieldPairList();

        //Loop through the fields in the table.
        //For each field, see if it matches a field in matching table
        for (Field field : this.fields) {
            SqlField tableField = RowField.of(this, field);

            for(SqlField matchingField : fieldsToMatch)
            {
                if (tableField.pairable(matchingField))
                {
                    pairs.add(new SqlFieldPair(tableField, matchingField));
                }
            }

        }

        return pairs;
    }

    public SqlFieldPairList getFieldPairs (SqlFieldMap<String, ? extends SqlField> fieldMapToMatch)
    {
        return getFieldPairs(fieldMapToMatch.values());
    }

    public static SqlTable of(TableId tableId)
    {
        return of(tableId, null);
    }

    public static SqlTable of(TableId tableId, String alias)
    {
        BqService bq = new BqService();
        Table table = bq.getTable(tableId);
        if (table == null) return null;

        return new SqlTable(table, alias);
    }

    public TableId getTableId() {
        return tableId;
    }

    public String getName() {
        return tableId.getTable();
    }

    public String getQualifiedName() {
        return String.format("%s.%s", tableId.getDataset(), tableId.getTable());
    }

    public String getNameAlias() {
        return String.format("%s %s", tableId.getTable(), getAlias());
    }

    public String getQualifiedNameAlias() {
        return SqlTable.toQualifiedNameAlias(tableId.getProject(), tableId.getDataset(), tableId.getTable(), getAlias());
    }

    public String getAlias()
    {
        return getAlias(true);
    }

    public String getAlias(Boolean useNameWhenNull) {

        if (alias!=null)
        {
            return alias;
        }
        else if (Boolean.TRUE.equals(useNameWhenNull))
        {
            return getName();
        }
        return null;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public static String toQualifiedNameAlias(String project, String dataset, String table, String alias)
    {
        return String.format("`%s.%s.%s` %s", project, dataset, table, alias);
    }
}
