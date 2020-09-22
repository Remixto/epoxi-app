package io.epoxi.app.engine.etl;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import io.epoxi.app.cloud.bq.BqService;
import io.epoxi.app.engine.EngineTestDataFactory;
import io.epoxi.app.engine.etl.pipeline.PipelineEndpoint;
import io.epoxi.app.engine.queue.action.Action;
import io.epoxi.app.repository.model.KeysetId;
import io.epoxi.app.repository.model.StepType;
import io.epoxi.app.util.validation.InvalidStateException;
import io.epoxi.app.repository.TestConfig;
import io.epoxi.app.repository.event.ETLTimestamp;
import io.epoxi.app.repository.event.EventKey;
import lombok.Getter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TransformPipelineTest {

    @Getter
    private List<KeysetId> keysets;

    StepType stepType = StepType.TRANSFORM;
    String accountName = TestConfig.etlTestAccountName;
    EngineTestDataFactory factory = new EngineTestDataFactory(accountName);

    public TransformPipelineTest() {

        keysets = new EngineTestDataFactory(TestConfig.engineTestAccountName).getTestKeysets();

         // Populate an array of steps to test
         for (KeysetId keysetId : keysets) {
             setupTransformTest(keysetId);
         }
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
    public void runTransform(KeysetId keysetId) {

        ETLStep step = getTransformStep(keysetId);

        assertNotNull(step, String.format("The Transform step for '%s' was successfully created", keysetId.toString()));
        assertDoesNotThrow(step::run, String.format("The Transform step for '%s' executed without unhanded error", keysetId.toString()));
        assertEquals(0, step.getNumErrors(), String.format("0 errors were logged during the processing of the actionBuffer for Transform step '%s'", keysetId.toString()));

    }

    public static void setupTransformTest(KeysetId keysetId)
    {

    }

    private ETLStep getTransformStep(KeysetId keysetId)
    {
        ETLTimestamp timestamp = factory.getTransformTestTimestamp();

        ETLStep step;

        try
        {
            //Try to create the step.  If it does not work because there is no preceding Extract, then call the Extract
            step = factory.getEtlStep(stepType, keysetId, timestamp);
            step.getPipeline().setTimestamp(timestamp); //Set an explicit timestamp to use within the pipeline
        }
        catch(InvalidStateException ex)
        {
            boolean endpointNotExists = ex.getViolations()
                                            .stream()
                                            .anyMatch(x -> x.getCondition().getClass().getSimpleName().equals(PipelineEndpoint.EndpointNotExistsCondition.class.getSimpleName()));

                if (endpointNotExists)
                {
                    ExtractPipelineTest extract = new ExtractPipelineTest();
                    extract.runExtract(keysetId);
                }

                step = factory.getEtlStep(stepType, keysetId, timestamp);
        }

        setEventKeys(step);

        return step;
    }

    private void setEventKeys(ETLStep step) {

        Action action = step.getActionBuffer().get(0);

        //Get the sql to return top 1000 rows from the table
        String keyFieldName = step.getPipeline().getSourceEndpoint().getKeyFieldId();
        TableId tableId = step.getPipeline().getSourceEndpoint().getTableId();
        String sql = String.format("SELECT %s FROM `%s` LIMIT 1000", keyFieldName, BqService.getQualifiedTable(tableId));
        TableResult result = new BqService().getQueryResult(sql);

        //Populate the action Keys
        List<EventKey> eventKeys = action.getEvent().getEventKeys();
        for (FieldValueList row : result.iterateAll())
        {
            eventKeys.add(new EventKey(row.get(0).getLongValue(), EventKey.Type.UPDATE));
        }
    }

    private static void clean() {

        // Clear the CDC queue
        EtlAllPipelinesTest.setup();
    }

    private static void init() {
        EngineTestDataFactory.init();
    }
}
