package io.epoxi.app.engine.etl;

import io.epoxi.app.engine.EngineTestDataFactory;
import io.epoxi.app.engine.etl.pipeline.PipelineEndpoint;
import io.epoxi.app.repository.model.KeysetId;
import io.epoxi.app.repository.model.StepType;
import io.epoxi.app.util.validation.InvalidStateException;
import io.epoxi.app.repository.TestConfig;
import io.epoxi.app.repository.event.ETLTimestamp;
import lombok.Getter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class LoadPipelineTest {

    @Getter
    private final List<KeysetId> keysets;

    StepType stepType = StepType.LOAD;
    String accountName = TestConfig.etlTestAccountName;
    EngineTestDataFactory factory = new EngineTestDataFactory(accountName);

    public LoadPipelineTest() {

        keysets = new EngineTestDataFactory(TestConfig.engineTestAccountName).getTestKeysets();
        // Populate an array of steps to test
        for (KeysetId keysetId : keysets) {
            setupLoadTest(keysetId);
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
    public void runLoad(KeysetId keysetId) {

        ETLStep step = getLoadStep(keysetId);
        runTransformIfNeeded(step);

        assertNotNull(step,
                String.format("The Load step for '%s' was successfully created", keysetId.toString()));
        assertDoesNotThrow(step::run,
                String.format("The Load step for '%s' executed without unhanded error", keysetId.toString()));
        assertEquals(0, step.getNumErrors(),
                String.format(
                        "0 errors were logged during the processing of the actionBuffer for Load step '%s'",
                        keysetId.toString()));
    }

    /**
     *  Check that the source endpoint of the step exists based on the known transform timestamp.
     *  If it doesn't then this is evidence that the preceding transform step needs to be run.
     * If so, do it now
     * @param step The step to be checked for a correct preceding step completion
     */
    private void runTransformIfNeeded(ETLStep step)
    {
        //Cache the existing timestamp
        PipelineEndpoint endpoint = step.getPipeline().getSourceEndpoint();
        ETLTimestamp oldTimestamp = endpoint.getTimestamp();  //Store the old timestamp so we can put it back later

        // Set the endpoint timestamp to that of a recent transform
        ETLTimestamp timestamp = step.getPipeline().getActionBuffer().get(0).getActionTimestamp();
        endpoint.setTimestamp(timestamp);

        //If the endpoint source is not valid, run the transform to create it.
        try{
            endpoint.validOrThrow();
        }
        catch(InvalidStateException ex)
        {
            boolean endpointNotExists = ex.getViolations()
            .stream()
            .anyMatch(x -> x.getCondition().getClass().getSimpleName().equals(PipelineEndpoint.EndpointNotExistsCondition.class.getSimpleName()));

            if (endpointNotExists)
            {
                TransformPipelineTest transform = new TransformPipelineTest();
                transform.runTransform(step.getKeysetId());
            }
        }
        finally{
            endpoint.setTimestamp(oldTimestamp);
        }
    }

    public static void setupLoadTest(KeysetId keysetId) {

    }

    private ETLStep getLoadStep(KeysetId keysetId)
    {
        // Get a timestamp of a recent transform
        ETLTimestamp timestamp = factory.getTransformTestTimestamp();
        return factory.getEtlStep(stepType, keysetId, timestamp);
    }

    private static void clean() {

        // Clear the CDC queue
        EtlAllPipelinesTest.setup();
    }

    private static void init() {
        EngineTestDataFactory.init();
    }
}
