package io.epoxi.app.etlengine.api;

import io.epoxi.app.repository.TestConfig;
import io.epoxi.app.repository.model.StepEndpointTemplate;
import org.junit.jupiter.api.*;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
 public class StepEndpointTemplateCrudTest {

    static EtlEngineApiTestDataFactory factory;
    static private Optional<StepEndpointTemplate> testObject = Optional.empty();
    static private String objectName;
    static private String objectType;

    @BeforeAll
    public static void setUp() {

        EtlEngineApiTestDataFactory.init();
        String accountName = TestConfig.apiTestAccountName;
        factory = new EtlEngineApiTestDataFactory(accountName);

        objectType = StepEndpointTemplate.class.getSimpleName();
        objectName = String.format("Test %s", objectType);
    }

    @Test @Order(1)
    public void addStepEndpointTemplateTest() {

        //If an existing item exists, delete it
        EtlEngineApiController api = EtlEngineApiTestDataFactory.getEngineConfigApiController();
        StepEndpointTemplate existingObj = api.getStepEndpointTemplate(objectName);
        if (existingObj !=null) api.deleteStepEndpointTemplate(existingObj.getId());

        //Create
        StepEndpointTemplate obj = EtlEngineApiTestDataFactory.createTestStepEndpointTemplate(objectName, true);

        //Assert
        assertNotNull(obj.getId().toString(), String.format("Fetched %s with the expected name", objectType));
    }

    @Test  @Order(2)
    public void getStepEndpointTemplateTest() {

        Long objectId = getTestObject().getId();

        EtlEngineApiController adminsApi  = EtlEngineApiTestDataFactory.getEngineConfigApiController();
        StepEndpointTemplate response = adminsApi.getStepEndpointTemplate(objectId);

        assertNotNull(response, String.format("Fetched %s", objectType));
        assertEquals(objectId, response.getId(), String.format("Fetched %s with the expected Id", objectType));
        assertNotNull(response.getName(), String.format("Fetched %s with a valid name", objectType));
    }

    @Test @Order(40)
    public void deleteStepEndpointTemplateTest() {

        testObject = Optional.empty(); //reset the test object
        assertDoesNotThrow(() -> deleteStepEndpointTemplate(false), String.format("Soft delete executed successfully for %s object", objectType));

        testObject = Optional.empty(); //reset the test object
        assertDoesNotThrow(() -> deleteStepEndpointTemplate(true), String.format("Hard delete executed successfully for %s object", objectType));
    }


    public void deleteStepEndpointTemplate(Boolean permanent) {

        EtlEngineApiController adminsApi  = EtlEngineApiTestDataFactory.getEngineConfigApiController();
        Long objectId = getTestObject().getId();
        adminsApi.deleteStepEndpointTemplate(objectId, permanent);

        assertNull(adminsApi.getStepEndpointTemplate(objectId), String.format("%s deleted successfully", objectType));
    }

    public StepEndpointTemplate getTestObject()
    {
        testObject = testObject
            .or(() -> Optional.ofNullable(new EtlEngineApiTestDataFactory(TestConfig.engineTestAccountName).getTestStepEndpointTemplate(objectName, true)));

        return testObject.get();
    }

}
