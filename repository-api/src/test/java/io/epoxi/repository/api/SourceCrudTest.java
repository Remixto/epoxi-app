package io.epoxi.repository.api;

import io.epoxi.repository.TestConfig;
import io.epoxi.repository.model.Source;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
 public class SourceCrudTest {

    static ApiTestDataFactory factory;
    static private Optional<Source> testObject = Optional.empty();
    static private String objectName;
    static private String objectType;

    @BeforeAll
    public static void setUp() {

        ApiTestDataFactory.init();
        String accountName = TestConfig.apiTestAccountName;
        factory = new ApiTestDataFactory(accountName);

        objectType = Source.class.getSimpleName();
        objectName = String.format("Test %s", objectType);
    }

    @Test @Order(1)
    public void addSourceTest() {

        //If an existing item exists, delete it
        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Source existingObj = devApi.getSource(objectName);
        if (existingObj !=null) devApi.deleteSource(existingObj.getId());

        //Create
        Source obj = factory.createTestSource(objectName, true);

        //Assert
        assertNotNull(obj.getId().toString(), String.format("Fetched %s with the expected name", objectType));
    }

    @Test  @Order(2)
    public void getSourceTest() {

        Long objectId = getTestObject().getId();

        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Source response = devApi.getSource(objectId);

        assertNotNull(response, String.format("Fetched %s", objectType));
        assertEquals(objectId, response.getId(), String.format("Fetched %s with the expected Id", objectType));
        assertNotNull(response.getName(), String.format("Fetched %s with a valid name", objectType));
    }

    @Test @Order(3)
    public void searchSourceTest() {

        //Search by name
        assertNotNull(getTestObject(), String.format("%s searched successfully for a specific name", objectType));

        DevelopersApiController devApi  = factory.getDevelopersApiController();

        //Search by pattern
        List<Source> response = devApi.searchSource(objectName.substring(2), null, null, 0, 1);
        assertNotNull(response, String.format("%s searched successfully", objectType));
        assertNotNull(response.get(0).getName(), String.format("%s searched successfully with matching pattern", objectType));

        //Retest search by name with a specific ProjectID
        Long projectId = response.get(0).getProjectId();
        List<Source> projectSpecificResponse = devApi.searchSource(null, objectName,projectId, 0, 1);
        assertNotNull(projectSpecificResponse.get(0).getName(), String.format("%s searched successfully with matching name within a specific project", objectType));
    }

    @Test @Order(40)
    public void deleteSourceTest() {

        testObject = Optional.empty(); //reset the test object
        assertDoesNotThrow(() -> deleteSource(false), String.format("Soft delete executed successfully for %s object", objectType));

        testObject = Optional.empty(); //reset the test object
        assertDoesNotThrow(() -> deleteSource(true), String.format("Hard delete executed successfully for %s object", objectType));
    }

    public void deleteSource(Boolean permanent) {

        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Long objectId = getTestObject().getId();
        devApi.deleteSource(objectId, permanent);

        assertNull(devApi.getSource(objectId), String.format("%s deleted successfully", objectType));
    }

    public Source getTestObject()
    {
        testObject = testObject
            .or(() -> Optional.ofNullable(factory.getTestSource(objectName, true)));

        return testObject.get();
    }

}
