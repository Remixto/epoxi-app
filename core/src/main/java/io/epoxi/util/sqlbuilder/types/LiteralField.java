package io.epoxi.util.sqlbuilder.types;

import com.google.cloud.bigquery.StandardSQLTypeName;

public class LiteralField extends SqlField {

    private final Object value;
    private final String name;
    private StandardSQLTypeName type;

    public LiteralField(Object value)
    {
        this(value, "[unknown]");
    }

    public LiteralField(Object value, String name)
    {
        this(value, name, null);
    }

    public LiteralField(Object value, String name, StandardSQLTypeName type)
    {
        this.value = value;
        this.name = name;
        this.type = type;
    }

    @Override
    public String getPath() {
        String stringVal;

        if (isNumeric(value) || isBoolean(value))
        {
            stringVal = value.toString();
        }
        else
        {
            stringVal = String.format("'%s'", value);
        }

        return stringVal;
    }

    @Override
    public String getPrefix()
    {
        return null;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public StandardSQLTypeName getType()
    {
        return type;
    }

    public void setType(StandardSQLTypeName type)
    {
        this.type= type;
    }

    private static boolean isNumeric(Object strNum) {
        if (strNum == null) {
            return false;
        }
        try {
            Double.parseDouble(strNum.toString());
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

    private static boolean isBoolean(Object strNum) {
        if (strNum == null) {
            return false;
        }
        try {
            Boolean.parseBoolean(strNum.toString());
        } catch (Exception nfe) {
            return false;
        }
        return true;
    }

}
