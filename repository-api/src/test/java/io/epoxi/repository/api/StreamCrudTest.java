package io.epoxi.repository.api;

import io.epoxi.repository.TestConfig;
import io.epoxi.repository.model.Stream;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StreamCrudTest {

    static ApiTestDataFactory factory;
    static private Optional<Stream> testObject = Optional.empty();
    static private String objectName;
    static private String objectType;

    @BeforeAll
    public static void setUp() {
        ApiTestDataFactory.init();
        String accountName = TestConfig.apiTestAccountName;
        factory = new ApiTestDataFactory(accountName);
        objectType = Stream.class.getSimpleName();
        objectName = String.format("Test %s", objectType);
    }

    @Test @Order(1)
    void addStreamTest() {

        //If an existing item exists, delete it
        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Stream existingObj = devApi.getStream(objectName);
        if (existingObj !=null) devApi.deleteStream(existingObj.getId());

        //Create
        Stream obj = factory.createTestSourceStream(objectName, "Firm_Data_Entity", true);

        //Assert
        assertNotNull(obj.getId().toString(), String.format("Fetched %s with the expected name", objectType));
    }

    @Test  @Order(2)
    public void getStreamTest() {

        Long objectId = getTestObject().getId();

        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Stream stream = devApi.getStream(objectId);

        assertNotNull(stream, String.format("Fetched %s", objectType));
        assertEquals(objectId, stream.getId(), String.format("Fetched %s with the expected Id", objectType));
        assertNotNull(stream.getName(), String.format("Fetched %s with a valid name", objectType));
    }

    @Test @Order(3)
    public void searchStreamTest() {

       getTestObject(); //get or create an object that we can test with

       //Search by name
       assertNotNull(getTestObject(), String.format("%s searched successfully for a specific name", objectType));

       DevelopersApiController devApi  = factory.getDevelopersApiController();

       //Search by pattern
       List<Stream> response = devApi.searchStream(objectName.substring(2), null, null, 0, 1);
       assertNotNull(response, String.format("%s searched successfully", objectType));
       assertNotNull(response.get(0).getName(), String.format("%s searched successfully with matching pattern", objectType));

       //Retest search by name with a specific ProjectID
       Long projectId = response.get(0).getProjectId();
       List<Stream> projectSpecificResponse = devApi.searchStream(null, objectName, projectId, 0, 1);
       assertNotNull(projectSpecificResponse.get(0).getName(), String.format("%s searched successfully with matching name within a specific project", objectType));

    }

    @Test @Order(40)
    public void deleteStreamTest() {

        testObject = Optional.empty(); //reset the test object
        assertDoesNotThrow(() -> deleteStream(false), String.format("Soft delete executed successfully for %s object", objectType));

        testObject = Optional.empty(); //reset the test object
        assertDoesNotThrow(() -> deleteStream(true), String.format("Hard delete executed successfully for %s object", objectType));
    }

    public void deleteStream(Boolean permanent) {

        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Long objectId = getTestObject().getId();
        devApi.deleteStream(objectId, permanent);

        assertNull(devApi.getStream(objectId), String.format("%s deleted successfully", objectType));
    }

    public Stream getTestObject()
    {
        testObject = testObject
            .or(() -> Optional.ofNullable(factory.getTestSourceStream(objectName, "Test_Source", true)));

        return testObject.get();
    }

}



