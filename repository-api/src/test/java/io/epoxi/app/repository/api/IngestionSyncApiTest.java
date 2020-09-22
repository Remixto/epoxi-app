package io.epoxi.app.repository.api;

import io.epoxi.app.cloud.scheduler.Schedule;
import io.epoxi.app.repository.TestConfig;
import io.epoxi.app.repository.model.IngestionSync;
import org.junit.jupiter.api.*;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
 class IngestionSyncApiTest {

    static ApiTestDataFactory factory;
    static private Optional<IngestionSync> testObject = Optional.empty();
    static private String objectName;
    static private String objectType;

    @BeforeAll
    public static void setUp() {

        ApiTestDataFactory.init();
        String accountName = TestConfig.apiTestAccountName;
        factory = new ApiTestDataFactory(accountName);

        objectType = IngestionSync.class.getSimpleName();
        objectName = String.format("Test %s", objectType);
    }

    @Test @Order(10)
    void addIngestionSyncWithNoScheduleTest() {

        //If there is an existing ingestionSync, delete it
        DevelopersApiController devApi = factory.getDevelopersApiController();
        IngestionSync existingObj = devApi.getIngestionSync(objectName, factory.getFirstProject().getId());
        if (existingObj !=null) devApi.deleteIngestionSync(existingObj.getId());

        //Create (without schedule)
        IngestionSync obj = factory.getTestIngestionSync("Test IngestionSync", "Firm_Data_Entity", true);
        assertDoesNotThrow(() -> devApi.addIngestionSync(obj), "No errors were thrown for ingestionSync (Queue)");
    }

    @Test @Order(11)
    void addIngestionSyncWithScheduleTest() {

         //If there is an existing ingestionSync, delete it
         DevelopersApiController devApi  = factory.getDevelopersApiController();
         IngestionSync existingObj = devApi.getIngestionSync(objectName, factory.getFirstProject().getId());
         if (existingObj !=null) devApi.deleteIngestionSync(existingObj.getId());

        //Create (with schedule)
        Schedule schedule = factory.createTestSchedule("Test Schedule");
        IngestionSync obj = factory.getTestIngestionSync("Test IngestionSync", "Firm_Data_Entity", schedule, true);

        assertDoesNotThrow( () -> devApi.addIngestionSync(obj), "No errors were thrown for ingestionSync (Queue)");
    }

    @Test @Order(20)
    void pauseIngestionSyncTest() {
        DevelopersApiController devApi = factory.getDevelopersApiController();

        IngestionSync obj = getTestObject();
        assertDoesNotThrow(() -> devApi.pauseIngestionSync(obj.getId()), "resumeIngestion Sync method completed without error");
    }

    @Test @Order(21)
    void resumeIngestionSyncTest() {
        DevelopersApiController devApi = factory.getDevelopersApiController();

        IngestionSync obj = getTestObject();
        assertDoesNotThrow(() -> devApi.resumeIngestionSync(obj.getId()), "resumeIngestion Sync method completed without error");
    }

    @Test @Order(30)
    void getIngestionSyncTest() {

        Long objectId = getTestObject().getId();

        DevelopersApiController devApi  = factory.getDevelopersApiController();
        IngestionSync response = devApi.getIngestionSync(objectId);

        assertNotNull(response, String.format("Fetched %s", objectType));
        assertEquals(objectId, response.getId(), String.format("Fetched %s with the expected Id", objectType));
        assertNotNull(response.getName(), String.format("Fetched %s with a valid name", objectType));
    }

    @Test @Order(40)
    void deleteIngestionSyncTest() {

        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Long objectId = getTestObject().getId();
        devApi.deleteIngestionSync(objectId);

        Assertions.assertNull(devApi.getIngestionSync(objectId), String.format("%s deleted successfully", objectType));
    }

    public IngestionSync getTestObject()
    {
        testObject = testObject
                .or(() -> Optional.ofNullable(factory.getTestIngestionSync(objectName, "Firm_Data_Entity", true, true)));

        return testObject.get();
    }
}
