package io.epoxi.repository.api;

import io.epoxi.repository.TestConfig;
import io.epoxi.repository.model.Target;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
 public class TargetCrudTest {

    static ApiTestDataFactory factory;
    static private Optional<Target> testObject = Optional.empty();
    static private String objectName;
    static private String objectType;

    @BeforeAll
    public static void setUp() {
        ApiTestDataFactory.init();
        String accountName = TestConfig.apiTestAccountName;
        factory = new ApiTestDataFactory(accountName);
        objectType = Target.class.getSimpleName();
        objectName = String.format("Test %s", objectType);
    }

    @Test @Order(1)
    public void addTargetTest() {

        //If an existing item exists, delete it
        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Target existingObj = devApi.getTarget(objectName);
        if (existingObj !=null) devApi.deleteTarget(existingObj.getId());

        //Create
        Target obj = factory.createTestTarget(objectName, true);

        //Assert
        assertNotNull(obj.getId().toString(), String.format("Fetched %s with the expected name", objectType));
    }

    @Test  @Order(2)
    public void getTargetTest() {

        Long objectId = getTestObject().getId();

        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Target response = devApi.getTarget(objectId);

        assertNotNull(response, String.format("Fetched %s", objectType));
        assertEquals(objectId, response.getId(), String.format("Fetched %s with the expected Id", objectType));
        assertNotNull(response.getName(), String.format("Fetched %s with a valid name", objectType));
    }

    @Test @Order(3)
    public void searchTargetTest() {

       //Search by name
       assertNotNull(getTestObject(), String.format("%s searched successfully for a specific name", objectType));

       DevelopersApiController devApi  = factory.getDevelopersApiController();

       //Search by pattern
       List<Target> response = devApi.searchTarget(objectName.substring(2), null, null, 0, 1);
       assertNotNull(response, String.format("%s searched successfully", objectType));
       assertNotNull(response.get(0).getName(), String.format("%s searched successfully with matching pattern", objectType));

       //Retest search by name with a specific ProjectID
       Long projectId = response.get(0).getProjectId();
       List<Target> projectSpecificResponse = devApi.searchTarget(null, objectName,projectId, 0, 1);
       assertNotNull(projectSpecificResponse.get(0).getName(), String.format("%s searched successfully with matching name within a specific project", objectType));
    }

    @Test @Order(40)
    public void deleteTargetTest() {

        testObject = Optional.empty(); //reset the test object
        assertDoesNotThrow(() -> deleteTarget(false), String.format("Soft delete executed successfully for %s object", objectType));

        testObject = Optional.empty(); //reset the test object
        assertDoesNotThrow(() -> deleteTarget(true), String.format("Hard delete executed successfully for %s object", objectType));
    }

    public void deleteTarget(Boolean permanent) {

        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Long objectId = getTestObject().getId();
        devApi.deleteTarget(objectId, permanent);

        assertNull(devApi.getTarget(objectId), String.format("%s deleted successfully", objectType));
    }

    public Target getTestObject()
    {

        testObject = testObject
            .or(() -> Optional.ofNullable(factory.getTestTarget(objectName, true)));

        return testObject.get();
    }

}
