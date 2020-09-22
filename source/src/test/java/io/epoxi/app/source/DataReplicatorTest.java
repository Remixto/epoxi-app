package io.epoxi.app.source;

import com.google.cloud.bigquery.TableId;
import io.epoxi.app.repository.TestConfig;
import io.epoxi.app.repository.TestDataFactory;
import io.epoxi.app.repository.model.Ingestion;
import io.epoxi.app.repository.model.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class DataReplicatorTest {

    public DataReplicatorTest() {
    }

    @BeforeAll
    public static void setup(){
        clean();
        init();
    }

    @AfterAll
    public static void tearDown() {

    }

    @Test
    public void replicateSource() {

        TestDataFactory factory = new TestDataFactory(TestConfig.engineTestAccountName);

        Ingestion ingestion = factory.getTestIngestion("Firm_Data_Entity", true);
        Stream stream = ingestion.getStreams().get(0);
        TableId target = TableId.of("warehouse-277017", "temp", "Entity");
        
        DataReplicator replicator = DataReplicator.newBuilder()
                                        .setStream(stream)
                                        .setTargetTable(target)
                                        .setPipelineTempDirectory(TestConfig.PIPELINE_TEMP_DIRECTORY)
                                        .setTargetTableExpiration(100)
                                        .build();
        assertDoesNotThrow(replicator::run, "The replication of source data was successful");

    }

    private static void clean() {
    }

    private static void init() {

    }
}