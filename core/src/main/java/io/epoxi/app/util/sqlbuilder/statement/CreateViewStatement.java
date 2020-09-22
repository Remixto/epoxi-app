package io.epoxi.app.util.sqlbuilder.statement;

import com.google.cloud.bigquery.TableId;

import io.epoxi.app.util.sqlbuilder.clause.SqlClause;
import io.epoxi.app.util.sqlbuilder.clause.SqlCreateViewClause;
import lombok.Getter;
import io.epoxi.util.sqlbuilder.clause.*;

public class CreateViewStatement  extends SqlStatement implements SqlClause {

    @Getter
    private TableId viewTableId;

    public static CreateViewStatement createView(TableId viewTableId, Boolean allowReplace, SelectStatement selectStatement)
    {
        CreateViewStatement statement = new CreateViewStatement();
        SqlCreateViewClause createView = new SqlCreateViewClause(viewTableId, allowReplace, selectStatement);
        statement.clauses.add(createView);
        statement.viewTableId = createView.getViewTableId();

        return statement;
    }



}
