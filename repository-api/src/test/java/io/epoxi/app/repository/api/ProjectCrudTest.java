package io.epoxi.app.repository.api;

import io.epoxi.app.repository.TestConfig;
import io.epoxi.app.repository.model.Project;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
 public class ProjectCrudTest {

    static ApiTestDataFactory factory;
    static private Optional<Project> testObject = Optional.empty();
    static private String objectName;
    static private String objectType;

    @BeforeAll
    public static void setUp() {

        ApiTestDataFactory.init();
        String accountName = TestConfig.apiTestAccountName;
        factory = new ApiTestDataFactory(accountName);

        objectType = Project.class.getSimpleName();
        objectName = String.format("Test %s", objectType);
    }

    @Test @Order(1)
    public void addProjectTest() {

        //If an existing item exists, delete it
        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Project existingObj = devApi.getProject(objectName);
        if (existingObj !=null) devApi.deleteProject(existingObj.getId());

        //Create
        Project obj = factory.createTestProject(objectName, true);

        //Assert
        assertNotNull(obj.getId().toString(), String.format("Fetched %s with the expected name", objectType));
    }

    @Test  @Order(2)
    public void getProjectTest() {

        Long objectId = getTestObject().getId();

        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Project response = devApi.getProject(objectId);

        assertNotNull(response, String.format("Fetched %s", objectType));
        assertEquals(objectId, response.getId(), String.format("Fetched %s with the expected Id", objectType));
        assertNotNull(response.getName(), String.format("Fetched %s with a valid name", objectType));
    }

    @Test @Order(3)
    public void searchProjectTest() {

        //Search by name
        assertNotNull(getTestObject(), String.format("%s searched successfully for a specific name", objectType));

        DevelopersApiController devApi  = factory.getDevelopersApiController();

        //Search by pattern
        List<Project> response = devApi.searchProject(objectName.substring(2), null, 0, 1);
        assertNotNull(response, String.format("%s searched successfully", objectType));
        assertNotNull(response.get(0).getName(), String.format("%s searched successfully with matching pattern", objectType));

    }

    @Test @Order(40)
    public void deleteProjectTest() {

        testObject = Optional.empty(); //reset the test object
        assertDoesNotThrow(() -> deleteProject(false), String.format("Soft delete executed successfully for %s object", objectType));

        testObject = Optional.empty(); //reset the test object
        assertDoesNotThrow(() -> deleteProject(true), String.format("Hard delete executed successfully for %s object", objectType));
    }

    public void deleteProject(Boolean permanent) {

        DevelopersApiController devApi  = factory.getDevelopersApiController();
        Long objectId = getTestObject().getId();
        devApi.deleteProject(objectId, permanent);

        assertNull(devApi.getProject(objectId), String.format("%s deleted successfully", objectType));
    }

    public Project getTestObject()
    {
        testObject = testObject
            .or(() -> Optional.ofNullable(factory.getTestProject(objectName, true)));

        return testObject.get();
    }



}
