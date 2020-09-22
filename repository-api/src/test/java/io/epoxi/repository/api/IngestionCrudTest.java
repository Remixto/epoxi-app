package io.epoxi.repository.api;

import io.epoxi.repository.TestConfig;
import io.epoxi.repository.model.Ingestion;
import io.epoxi.repository.api.model.ControllerBase;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
 public class IngestionCrudTest {

    static ApiTestDataFactory factory;
    static private Optional<Ingestion> testObject = Optional.empty();
    static private String objectName;
    static private String objectType;

    @BeforeAll
    public static void setUp() {

        ApiTestDataFactory.init();
        String accountName = TestConfig.apiTestAccountName;
        factory = new ApiTestDataFactory(accountName);

        objectType = Ingestion.class.getSimpleName();
        objectName = String.format("Test %s", objectType);
    }

    @Test @Order(10)
    public void addIngestionTestWithNamedStreams() {

        //If there is an existing ingestion, delete it
        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Ingestion existingObj = devApi.getIngestion(objectName, factory.getFirstProject().getId());
        if (existingObj !=null) devApi.deleteIngestion(existingObj.getId());

        //Create
        Ingestion obj = factory.createTestIngestion(objectName, false, true);

        //Assert
        Assertions.assertNotNull(obj.getId().toString(), String.format("Fetched %s with the expected name", objectType));
    }

    @Test @Order(11)
    public void addIngestionTestWithAnonymousStreams() {

        //If there is an existing ingestion, delete it
        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Ingestion existingObj = devApi.getIngestion(objectName, factory.getFirstProject().getId());
        if (existingObj !=null) devApi.deleteIngestion(existingObj.getId());

        //Create
        Ingestion obj = factory.createTestIngestion(objectName, true, true);

        //Assert
        Assertions.assertNotNull(obj.getId().toString(), String.format("Fetched %s with the expected name", objectType));
    }

    @Test @Order(20)
    public void getIngestionTest() {

        Long objectId = getTestObject().getId();

        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Ingestion response = devApi.getIngestion(objectId);

        assertNotNull(response, String.format("Fetched %s", objectType));
        Assertions.assertEquals(objectId, response.getId(), String.format("Fetched %s with the expected Id", objectType));
        Assertions.assertNotNull(response.getName(), String.format("Fetched %s with a valid name", objectType));
    }


    @Test @Order(30)
    public void searchIngestionTest() {

        //Search by name
        Ingestion ingestion = getTestObject();
        assertNotNull(ingestion, String.format("%s searched successfully for a specific name", objectType));

        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Long projectId = ingestion.getProjectId();

        //Search by pattern
        List<Ingestion> response = devApi.searchIngestion(objectName.substring(2), null, null, 0, 1);
        assertNotNull(response, String.format("%s searched successfully", objectType));
        Assertions.assertNotNull(response.get(0).getName(), String.format("%s searched successfully with matching pattern", objectType));

        //Retest search by name with a specific ProjectID
        List<Ingestion> projectSpecificResponse = devApi.searchIngestion(null, objectName,projectId, 0, 1);
        Assertions.assertNotNull(projectSpecificResponse.get(0).getName(), String.format("%s searched successfully with matching name within a specific project", objectType));
    }

     @Test @Order(40)
    public void deleteIngestionTest() {

        testObject = Optional.empty(); //reset the test object
        assertDoesNotThrow(() -> deleteIngestion(false), String.format("Soft delete executed successfully for %s object", objectType));
        testObject = Optional.empty(); //reset the test object
        assertDoesNotThrow(() -> deleteIngestion(true), String.format("Hard delete executed successfully for %s object", objectType));
    }

    @Test @Order(50) @Disabled
    public void undeleteIngestionTest() {

        //Create a test item and soft delete it
        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Long objectId = getTestObject().getId();
        devApi.deleteIngestion(objectId, false);

        //Undelete it
        devApi.undeleteIngestion(objectId);

        //Assert that the item is back
        Ingestion ingestion = devApi.getIngestion(objectId);
        assertNotNull(ingestion, "Ingestion has been successfully undeleted");

    }

    @Test @Order(60)
    public void patchIngestionTest() {

        //Place a message on the queue
        ApiTestDataFactory apiFactory = new ApiTestDataFactory(TestConfig.engineTestAccountName);
        Ingestion ingestion = apiFactory.getTestIngestion("Firm_Data_Entity", false);
        Long ingestionId = ingestion.getId();

        DevelopersApiController devApi = apiFactory.getDevelopersApiController();
        assertDoesNotThrow(()-> devApi.patchIngestion(ingestionId, ControllerBase.IngestionOperation.START), "Ingestion queued successfully");
    }

    public void deleteIngestion(Boolean permanent) {

        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Long objectId = getTestObject().getId();
        devApi.deleteIngestion(objectId, permanent);

        Assertions.assertNull(devApi.getIngestion(objectId), String.format("%s deleted successfully", objectType));
    }

    public Ingestion getTestObject()
    {
        testObject = testObject
                .or(() -> Optional.ofNullable(factory.getTestIngestion(objectName, true)));

        return testObject.get();
    }
}
