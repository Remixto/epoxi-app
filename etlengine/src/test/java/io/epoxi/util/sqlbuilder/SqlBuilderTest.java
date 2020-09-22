package io.epoxi.util.sqlbuilder;

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import io.epoxi.cloud.bq.BqService;
import io.epoxi.util.sqlbuilder.statement.MergeStatement;
import io.epoxi.util.sqlbuilder.statement.SelectStatement;
import io.epoxi.util.sqlbuilder.types.*;
import io.epoxi.util.sqlbuilder.types.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlBuilderTest {

    BqService bqService = new BqService("warehouse-277017");

    @BeforeAll
    static void setup()
    {

        clean();
        init();
    }

    @AfterAll
    public static void tearDown() {

    }

    @Test
    void testSqlFieldPairable()
    {
        CodeField codeField = new CodeField("CURRENT_DATETIME()", "Date_Created", StandardSQLTypeName.DATETIME);
        RowField rowField1 = SqlTable.of(TableId.of("etlengine", "test", "PairableTest1")).getField("DateField");
        RowField rowField2 = SqlTable.of(TableId.of("etlengine", "test", "PairableTest2")).getField("DateField");

        assertTrue(rowField1.pairable(codeField), "rowField1 pairs with codeField");
        assertTrue(codeField.pairable(rowField1), "rowField1 pairs with codeField");
        assertTrue(rowField2.pairable(rowField1), "rowField2 pairs with rowField1");
        assertTrue(rowField1.pairable(rowField2), "rowField1 pairs with rowField2");
    }

    @Test
    void testSqlFieldNotPairable()
    {
        CodeField codeField1 = new CodeField("CURRENT_DATETIME()", "Date_Last_Modified", StandardSQLTypeName.DATETIME);
        CodeField codeField2 = new CodeField("CURRENT_DATETIME()", "DateField", StandardSQLTypeName.STRING);
        RowField rowField = SqlTable.of(TableId.of("etlengine", "test", "PairableTest1")).getField("DateField");

        assertTrue(rowField.pairable(codeField1), "rowField does not pair codeField1");
        assertTrue(codeField1.pairable(rowField), "rowField does not pair codeField1");
        assertTrue(rowField.pairable(codeField2), "rowField does not pair codeField2");
        assertTrue(codeField1.pairable(codeField2), "rowField does not pair codeField2");

    }

    @Test
    void getSqlBuilder_SelectStatement()
    {

        SqlTable tEntity = SqlTable.of(TableId.of("store", "Entity"), "e");
        SqlTable tEntityType = SqlTable.of(TableId.of("store", "Entity_Type"), "et");

        SqlField e1 = tEntity.getField("Legal_Name");
        SqlField e2 = new LiteralField("Brent", "First_Name");
        SqlPredicate wherePredicate = new SqlPredicate(e1, "=", e2);

        SqlFieldMap<String, SqlField> fields = SqlFieldMap.newBuilder()
                                                .add(tEntity.getFields("Entity_Key","Entity_Name","Entity_Type_Key"))
                                                .add(tEntity.getFields("Legal_Name"))
                                                .build();

        SelectStatement query = SelectStatement
            .select( fields)
            .from(tEntity)
                .innerJoin(tEntityType, "Entity_Type_Key", "Entity_Type_Key")
                .where(wherePredicate);

        assertDoesNotThrow(() -> query.validate(bqService), "The Select query is valid");

    }

    @Test
    void getSqlBuilder_MergeStatement()
    {
        SqlTable tSource = SqlTable.of(TableId.of("etl_load", "Firm_Data_Entity_Type"), "s");
        SqlTable tTarget = SqlTable.of(TableId.of("store", "Entity_Type"), "t");

        MergeStatement query = ETLBuilder.getETLLoadMergeStatement(tSource,  tTarget, "Entity_Type_Key");

        assertDoesNotThrow(() -> query.validate(bqService), "The Merge Statement query is valid");
    }

    private static void clean() {

        //Clear the CDC queue
        UtilTestSuite.setup();

    }

    private static void init() {


    }
}
