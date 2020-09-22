package io.epoxi.util.sqlbuilder.types;

import com.google.cloud.bigquery.StandardSQLTypeName;

public abstract class SqlField {

    public abstract StandardSQLTypeName getType();

    /**
     * The field in the database from which data will be retrieved.
     */
    public abstract String getPath();

    /**
     * A path prefix (often referred to as a table alias)
     */
    public abstract String getPrefix();

    /**
     * The column name that will be outputted in the returned resultSet.  This value is used as a table alias.
     */
    public abstract String getName();


     /**
     * Returns a smart concatenation of getPath() and getName().
     */
    public String getPathName()
    {
        String fieldString = getPath();
        if (getName()!=null && !getPath().equals(getName()))
        {
            fieldString = String.format("%s as %s", fieldString, getName());
        }
        return fieldString;
    }

     /**
     * Returns a smart concatenation of getPrefix() and getPath().
     */
    public String getPrefixPath()
    {
        String fieldName = getPath();

        if (getPrefix()!=null)
        {
            fieldName = String.format("%s.%s", getPrefix(), fieldName);
        }

        return fieldName;
    }

    /**
     * Returns a smart concatenation of getPath(), getPath() and getName().
     */
    public String getPrefixPathName()
    {
        String fieldString = getPrefixPath();
        if (getName()!=null && !getPath().equals(getName()))
        {
            fieldString = String.format("%s as %s", fieldString, bqEscape(getName()));
        }

        return fieldString;
    }

    /**
     * Escape all reserved words in BigQuery
     * @param stringToEscape The string to escape
     * @return An escaped string
     */
    protected static String bqEscape (String stringToEscape)
    {
        if (stringToEscape.equalsIgnoreCase("desc"))  //TODO - Expand this list to include all the reserved words.  Make the compare with RegEx
            stringToEscape = String.format("`%s`", stringToEscape);

        return stringToEscape;
    }

    public boolean pairable(Object obj) {

        if (obj == this) { return true; }
        if (obj == null ) { return false; }

        if(obj.getClass() != this.getClass() && !SqlField.class.isAssignableFrom(obj.getClass())){ return false; }

        SqlField rowFieldToCompare = (SqlField) obj;

        //Match the alias and the type of each field
        return (    rowFieldToCompare.getName().equals(this.getName())
                && rowFieldToCompare.getType().equals(this.getType())
            );


    }


}
