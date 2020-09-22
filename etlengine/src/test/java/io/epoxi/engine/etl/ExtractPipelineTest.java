package io.epoxi.engine.etl;

import io.epoxi.cloud.bq.BqService;
import io.epoxi.engine.EngineTestDataFactory;
import io.epoxi.repository.model.KeysetId;
import io.epoxi.repository.model.StepType;
import io.epoxi.repository.TestConfig;
import lombok.Getter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ExtractPipelineTest {

    @Getter
    private final List<KeysetId> keysets = new ArrayList<>();

    StepType stepType = StepType.EXTRACT;
    String accountName = TestConfig.etlTestAccountName;
    EngineTestDataFactory factory = new EngineTestDataFactory(accountName);

    public ExtractPipelineTest() {
    }

    @BeforeAll
    public static void setup() {
        clean();
        init();
    }

    @AfterAll
    public static void tearDown() {

    }

    @ParameterizedTest
    @MethodSource("io.epoxi.engine.etl.EtlAllPipelinesTest#getKeysets")
    public void runExtract(KeysetId keysetId) {

        setupExtractTest(keysetId);

        ETLStep step = factory.getEtlStep(stepType, keysetId);

        assertNotNull(step, String.format("The Extract step for '%s' was successfully created", keysetId.toString()));
        assertDoesNotThrow(step::run, String.format("The Extract step for '%s' executed without unhanded error", keysetId.toString()));
        assertEquals(0, step.getNumErrors(), "0 errors were logged during the processing of the actionBuffer for Extract step");

    }

    private static void clean() {
        // Clear the CDC queue
        EtlAllPipelinesTest.setup();
        BqService bq = new BqService();
        bq.executeQuery("DELETE FROM `warehouse-277017.etl_resource.Extract_CDC` WHERE 1 = 1");
    }

    private static void init() {
        EngineTestDataFactory.init();
    }


    public static void setupExtractTest(KeysetId keysetId)
    {
        //Update data in the source data
        String sql = "UPDATE `firm-data.common.{TableName}` SET Date_Last_Modified = CURRENT_TIMESTAMP() WHERE {TableName}_ID IN (SELECT {TableName}_ID FROM `firm-data.common.{TableName}` LIMIT 20)";
        sql = sql.replace("{TableName}", keysetId.getKeysetName().replace("Firm_Data_", ""));
        BqService bq = new BqService();
        bq.executeQuery(sql);
    }

}
