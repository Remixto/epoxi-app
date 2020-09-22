package io.epoxi.app.util.sqlbuilder.types;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.Field.Mode;

import java.util.ArrayList;
import java.util.List;

public final class RowField extends SqlField {

    //protected final Field field;

    private final String name;
    private final String path;
    private final SqlTable table;
    private RowField parentField;
    private final List<RowField> subfields;
    private String description;
    private StandardSQLTypeName type;
    private Mode mode;

    protected RowField(SqlTable table, Field field)
    {
        this(table, field, null);
    }

    protected RowField(SqlTable table, Field field, String name)
    {
        this(table, field, name, null);
    }

    protected RowField(SqlTable table, Field field, String name, RowField parentField)
    {
        if (field == null || table == null)
        {
            throw new IllegalArgumentException();
        }

        this.table = table;
        this.name = name;
        this.path = field.getName();
        this.description = field.getDescription();
        this.mode = field.getMode();
        this.type = field.getType().getStandardType();
        this.parentField = parentField;

        this.subfields = new ArrayList<>();

        if (field.getSubFields() != null)
        {
            for(Field subfield : field.getSubFields())
            {
                subfields.add(RowField.of(table, subfield, null, this));
            }
        }
    }

    protected RowField(SqlTable table, String path, String name, List<RowField> subFields)
    {
        if (table == null || name == null || subFields == null)
        {
            throw new IllegalArgumentException();
        }

        this.table = table;
        this.name = name;
        this.path = path;
        this.type = StandardSQLTypeName.STRUCT;
        this.subfields = subFields;

    }

    public String getPath()
    {
         return getPath(true);
    }

    public String getPath(Boolean prefixParent)
    {
       String fieldName = path;
       if (parentField !=null && prefixParent) fieldName = String.format("%s.%s", parentField.getName(), fieldName);

       fieldName = SqlField.bqEscape(fieldName);

       return fieldName;
    }

    @Override
    public String getPrefix()
    {
        return table.getAlias();
    }

    @Override
    public String getName()
    {
        String fieldName = name;

        if (fieldName==null)
        {
            fieldName = getPath();
        }

        return fieldName;
    }

    public SqlTable getTable()
    {
        return table;
    }

    public TableId getTableId()
    {
        return table.getTableId();
    }

    public String getDescription()
    {
        return description;
    }

    public StandardSQLTypeName getType()
    {
        return type;
    }

    public void setType(StandardSQLTypeName type) {
        this.type = type;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<RowField> getSubfields()
    {
        return subfields;
    }

    /**
     * Indicates whether the RowField is a subfield.
     */
    public Boolean isSubfield()
    {
        return (parentField !=null);
    }

    /**
     * Return a (shallow) clone of the RowField with a new name
     * @param name The name of the new RowField
     * @return a new RowField based on the existing RowField, except with a new name
     */
    public RowField clone(String name)
    {
        Field field = Field.of(path, type);
        RowField rowField = RowField.of(table, field, name);
        rowField.description = description;
        rowField.mode = mode;
        rowField.type = type;
        rowField.parentField = parentField;
        rowField.subfields.addAll(subfields);

        return rowField;
    }


    /**
     * Creates a new RowField using a BiqQuery Field object.
     * @param table The table in which the field exists
     * @param field  The BigQuery field object whose properties will be referenced by the new RowField
     * @return a new RowField object that references an internal FieldObject
     */
    public static RowField of(SqlTable table, Field field)
    {
        return new RowField(table, field);
    }

    /**
     * Creates a new RowField using a BiqQuery Field object.
     * @param table The table in which the field exists
     * @param field  The BigQuery field object whose properties will be referenced by the new RowField
     * @param name  The name by which the RowField will be called.  It can be different than the name of the internal Field object.
     * In such cases, the name acts as an alias for the field in any Sql generated.
     * @return a new RowField object that references an internal FieldObject
     */
    public static RowField of(SqlTable table, Field field, String name)
    {
        return new RowField(table, field, name);
    }

    /**
     * Creates a new RowField using a BiqQuery Field object.
     * @param table The table in which the field exists
     * @param field  The BigQuery field object whose properties will be referenced by the new RowField
     * @param name  The name by which the RowField will be called.  It can be different than the name of the internal Field object.
     * In such cases, the name acts as an alias for the field in any Sql generated.
     * @param parentField  When the field is a subfield, this value specifies the parent field to which the subfield is a member.
     * @return a new RowField object that references an internal FieldObject
     */
    public static RowField of(SqlTable table, Field field, String name, RowField parentField)
    {
        return new RowField(table, field, name, parentField);
    }

    public static RowField ofNest(SqlTable table, String path, String name, List<RowField> fieldsToNest)
    {
        //Get a new RowField
        return new RowField(table, path, name, fieldsToNest);
    }

    /**
     * Creates a new List<RowField> object, where each RowField is assigned to the table
     * @param table  Specifies the SqlTable to which the new RowFields will belong.
     * @param fieldList  The BigQuery FieldList object that contains the Field objects on which RowFields will be created
     * @return a list of RowFields
     */
    public static List<RowField> ofList(SqlTable table, FieldList fieldList)
    {
        return ofList( fieldList, table, null);
    }

     /**
     * Creates a new List<RowField> object.  This is helper method that is used for creating lists of RowFields.
     * It is for internal use only, as it contains parameters that must be called a certain way by other methods in class.
     * To call this method effectively, you must pass either a table, or a parentField, but not both.
     * @param fieldList  The BigQuery FieldList object that contains the Field objects on which RowFields will be created
     * @param table The table in which the field exists.
     * @param parentField  When the field is a subfield, this value specifies the parent field to which the subfield is a member.
     * Passing this field will cause the any table parameter passed to be ignored.

     * @return a list of RowFields
     */
    private static List<RowField> ofList(FieldList fieldList, SqlTable table, RowField parentField)
    {
        List<RowField> list = new ArrayList<>();

        SqlTable t = table;
        if (parentField !=null) t = parentField.table;

        for(Field field : fieldList)
        {
            list.add(RowField.of(t, field, null, parentField));
        }

        return list;
    }


    public boolean pairable(Object obj) {

        if (!super.pairable(obj))
        {
            return false;
        }

        //If there are no subfields, return true, that is as far as we need to go
        if (this.getSubfields() ==null || this.getSubfields().size() == 0) return true;

        //Subfields exist.  Lets check they all match

        //Subfield Check 1: if the field being compared is not a RowField, then it doesn't have subfields and can't possibly match.  Return false
        if (obj.getClass() != this.getClass()) { return false; }

        //Subfield Check 2: Check the subfields are the same and in the same order
        RowField rowFieldToCompare = (RowField) obj;

        //If there are subfields, compare each of them as well, matching the order in both sets
        if (this.getSubfields() == null) return true;

        int index = 0;
        for(RowField subRowField :  this.getSubfields())
        {
            RowField subRowFieldToCompare = rowFieldToCompare.getSubfields().get(index);
            if (!subRowField.pairable(subRowFieldToCompare))
            {   return false;
            }
            index ++;
        }
        return true;

    }

    public static RowFieldBuilder newBuilder()
    {
        return new RowFieldBuilder();
    }

    /*********************************************************
     * RowFieldBuilder
     ********************************************************/

    public static class RowFieldBuilder
    {
        private SqlTable table;
        private String name;
        private LegacySQLTypeName type;
        private List<Field> subfields;

        public RowFieldBuilder setTable(SqlTable table)
        {
            this.table = table;
            return this;
        }


        public RowFieldBuilder setName(String name)
        {
            this.name = name;
            return this;
        }

        public RowFieldBuilder setType(LegacySQLTypeName type)
        {
            this.type = type;
            return this;
        }

        public RowFieldBuilder setSubfields(List<Field> subfields)
        {
            this.subfields = subfields;
            return this;
        }

        public RowField build()
        {
            Field field = Field.of(name, type, subfields.toArray(new Field[subfields.size()]));

            return new RowField(table, field, name);
        }
    }



}
