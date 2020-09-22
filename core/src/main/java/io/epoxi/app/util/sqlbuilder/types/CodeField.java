package io.epoxi.app.util.sqlbuilder.types;

import com.google.cloud.bigquery.StandardSQLTypeName;

public class CodeField extends SqlField {

    final String sqlExpression;
    String name;
    StandardSQLTypeName type;

    public CodeField(String sqlExpression)
    {
        this(sqlExpression, "[unknown]");
    }

    public CodeField(String sqlExpression, String name)
    {
        this(sqlExpression, name, null);
    }

    public CodeField(String sqlExpression, String name, StandardSQLTypeName type)
    {
        this.sqlExpression = sqlExpression;
        this.name = name;
        this.type = type;
    }

    @Override
    public String getPath() {
        return sqlExpression;
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

}
