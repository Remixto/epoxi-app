package io.epoxi.repository.model;

import io.epoxi.cloud.scheduler.Schedule;
import io.epoxi.repository.IngestionSyncRepository;
import io.epoxi.repository.TestConfig;
import io.epoxi.repository.TestDataFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class IngestionSyncTest {
    static TestDataFactory factory;
    static private Optional<IngestionSync> testObject = Optional.empty();
    static private String objectName;
    static private String objectType;

    @BeforeAll
    public static void setUp() {

        TestDataFactory.init();
        String accountName = TestConfig.apiTestAccountName;
        factory = new TestDataFactory(accountName);

        objectType = IngestionSync.class.getSimpleName();
        objectName = String.format("Test %s", objectType);
    }

    @Test @Order(10)
    public void addIngestionSyncWithNoScheduleTest() {

        IngestionSyncRepository repository = factory.getAccountRepository().getIngestionSyncRepository();

        //Delete any existing ingestionSync
        IngestionSync ingestionSync = repository.get(objectName);
        if (ingestionSync!=null) repository.delete(ingestionSync);

        //Create (without schedule)
        IngestionSync obj = factory.getTestIngestionSync("Test IngestionSync", "Firm_Data_Entity", true);
        repository.add(obj);

        assertDoesNotThrow(() -> repository.add(obj), "No errors were thrown for ingestionSync (Queue)");
    }

    @Test @Order(11)
    public void addIngestionSyncWithScheduleTest() {

        IngestionSyncRepository repository = factory.getAccountRepository().getIngestionSyncRepository();

        //Delete any existing ingestionSync
        IngestionSync ingestionSync = repository.get(objectName);
        if (ingestionSync!=null) repository.delete(ingestionSync);

        //Create (with schedule)
        Schedule schedule = factory.createTestSchedule("Test Schedule");
        IngestionSync obj = factory.getTestIngestionSync("Test IngestionSync", "Firm_Data_Entity", schedule, true);

        assertDoesNotThrow( () -> repository.add(obj), "No errors were thrown for ingestionSync (Queue)");
    }

    @Test @Order(30)
    public void getIngestionSyncTest() {

        Long objectId = getTestObject().getId();
        IngestionSyncRepository repository = factory.getAccountRepository().getIngestionSyncRepository();
        IngestionSync response = repository.get(objectId);

        assertNotNull(response, String.format("Fetched %s", objectType));
        assertEquals(objectId, response.getId(), String.format("Fetched %s with the expected Id", objectType));
        assertNotNull(response.getName(), String.format("Fetched %s with a valid name", objectType));
    }

    @Test @Order(40)
    public void deleteIngestionSyncTest() {

        Long objectId = getTestObject().getId();
        IngestionSyncRepository repository = factory.getAccountRepository().getIngestionSyncRepository();
        repository.delete(objectId);

        Assertions.assertNull(repository.get(objectId), String.format("%s deleted successfully", objectType));
    }

    public IngestionSync getTestObject()
    {
        testObject = testObject
                .or(() -> Optional.ofNullable(factory.getTestIngestionSync(objectName, "Firm_Data_Entity", true, true)));

        return testObject.get();
    }
}