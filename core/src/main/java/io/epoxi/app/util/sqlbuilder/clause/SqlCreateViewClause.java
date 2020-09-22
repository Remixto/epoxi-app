package io.epoxi.app.util.sqlbuilder.clause;

import com.google.cloud.bigquery.TableId;

import io.epoxi.app.util.sqlbuilder.statement.SelectStatement;

public class SqlCreateViewClause implements SqlClause {

    final TableId viewTableId;
    final Boolean allowReplace;
    final SelectStatement selectStatement;

    public SqlCreateViewClause (TableId viewTableId, Boolean allowReplace, SelectStatement selectStatement)
    {
        this.viewTableId = viewTableId;
        this.allowReplace = allowReplace;
        this.selectStatement = selectStatement;
    }

    public String getSql()
    {
        return String.format("CREATE%sVIEW     `%s.%s` AS%n", orReplace(allowReplace), viewTableId.getDataset(), viewTableId.getTable()) +
            selectStatement.getSql(false);
    }

    public TableId getViewTableId() {
        return viewTableId;
    }

    public String orReplace(Boolean allowReplace)
    {
        String orReplace = " ";
        if (Boolean.TRUE.equals(allowReplace))
        {
            orReplace = " OR REPLACE ";
        }

        return orReplace;
    }

}
