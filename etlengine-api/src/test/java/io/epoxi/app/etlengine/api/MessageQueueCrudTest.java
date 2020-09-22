package io.epoxi.app.etlengine.api;

import io.epoxi.app.repository.TestConfig;
import io.epoxi.app.repository.model.MessageQueue;
import org.junit.jupiter.api.*;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
 public class MessageQueueCrudTest {

    static EtlEngineApiTestDataFactory factory;
    static private Optional<MessageQueue> testObject = Optional.empty();
    static private String objectName;
    static private String objectType;

    @BeforeAll
    public static void setUp() {

        EtlEngineApiTestDataFactory.init();
        String accountName = TestConfig.apiTestAccountName;
        factory = new EtlEngineApiTestDataFactory(accountName);

        objectType = MessageQueue.class.getSimpleName();
        objectName = String.format("Test %s", objectType);
    }

    @Test @Order(1)
    public void addMessageQueueTest() {

        //If an existing item exists, delete it
        EtlEngineApiController api = EtlEngineApiTestDataFactory.getEngineConfigApiController();
        MessageQueue existingObj = api.getMessageQueue(objectName);
        if (existingObj !=null) api.deleteMessageQueue(existingObj.getId());

        //Create
        MessageQueue obj = EtlEngineApiTestDataFactory.createTestMessageQueue(objectName, true);

        //Assert
        assertNotNull(obj.getId().toString(), String.format("Fetched %s with the expected name", objectType));
    }

    @Test @Order(2)
    public void getMessageQueueTest() {

        Long objectId = getTestObject().getId();

        EtlEngineApiController adminsApi  = EtlEngineApiTestDataFactory.getEngineConfigApiController();
        MessageQueue response = adminsApi.getMessageQueue(objectId);

        assertNotNull(response, String.format("Fetched %s", objectType));
        assertEquals(objectId, response.getId(), String.format("Fetched %s with the expected Id", objectType));
        assertNotNull(response.getName(), String.format("Fetched %s with a valid name", objectType));
    }

    @Test @Order(40)
    public void deleteMessageQueueTest() {

        testObject = Optional.empty(); //reset the test object
        assertDoesNotThrow(() -> deleteMessageQueue(false), String.format("Soft delete executed successfully for %s object", objectType));

        testObject = Optional.empty(); //reset the test object
        assertDoesNotThrow(() -> deleteMessageQueue(true), String.format("Hard delete executed successfully for %s object", objectType));
    }

    public void deleteMessageQueue(Boolean permanent) {

        EtlEngineApiController adminsApi  = EtlEngineApiTestDataFactory.getEngineConfigApiController();
        Long objectId = getTestObject().getId();
        adminsApi.deleteMessageQueue(objectId, permanent);

        assertNull(adminsApi.getMessageQueue(objectId), String.format("%s deleted successfully", objectType));
    }

    public MessageQueue getTestObject()
    {
        testObject = testObject
                .or(() -> Optional.ofNullable(new EtlEngineApiTestDataFactory(TestConfig.engineTestAccountName).getTestMessageQueue(objectName, true)));

        return testObject.get();
    }
}
