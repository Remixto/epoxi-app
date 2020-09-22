package io.epoxi.cloud.bq;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class BQServiceTest
{
    @Test
    void getTableSchemaTest()
    {
        BqService bq = new BqService("warehouse-277017");
        Table table = bq.getTable("warehouse-277017", "store", "Entity");

        TableDefinition def = table.getDefinition();
        Schema schema = def.getSchema();

        assert schema != null;
        assertEquals("Entity_Key", schema.getFields().get(1).getName(), "Fetched the schema from the store.Entity table");
    }

    @Test
    void getTable()
    {
        BqService bq = new BqService("warehouse-277017");
        Table table1 = bq.getTable("store", "Entity");
        assertNotNull(table1, "Fetched a BiqQuery table successfully using string parts");

        Table table2 = bq.getTable("warehouse-277017", "store", "Entity");
        assertNotNull(table2, "Fetched a BiqQuery table successfully using string parts");

        TableId tableId = TableId.of("warehouse-277017", "store", "Entity");
        Table table3 = bq.getTable(tableId);
        assertNotNull(table3, "Fetched a BiqQuery table successfully using a TableId");
    }
}
