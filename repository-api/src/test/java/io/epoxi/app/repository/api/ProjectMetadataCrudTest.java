package io.epoxi.app.repository.api;

import io.epoxi.app.repository.TestConfig;
import io.epoxi.app.repository.model.Metadata;
import io.epoxi.app.repository.model.Project;
import org.junit.jupiter.api.*;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
 public class ProjectMetadataCrudTest {

    static ApiTestDataFactory factory;
    static private Optional<Metadata> testObject = Optional.empty();
    static private String objectItemKey;
    static private String objectType;

    @BeforeAll
    public static void setUp() {

        ApiTestDataFactory.init();

        //Get (or create) a project metadata info
        String accountName = TestConfig.apiTestAccountName;
        factory = new ApiTestDataFactory(accountName);

        objectType = Metadata.class.getSimpleName();
        objectItemKey = String.format("Test %s", objectType);
    }

    @Test @Order(1)
    public void addProjectMetadataTest() {

        //If an existing item exists, delete it
        AdminsApiController api  = ApiTestDataFactory.getAdminsApiController();
        api.deleteProjectMetadata(factory.getFirstProject().getId(), objectItemKey);

        //Create
        Metadata obj = factory.createTestProjectMetadataItem(objectItemKey, "test value");

        //Assert
        assertNotNull(obj, String.format("Fetched %s", objectType));
    }

     @Test  @Order(2)
    public void getProjectMetadataTest() {

        Project project = factory.getFirstProject();

        //Search by name
        String objectKey = getTestObject().getKey();

        AdminsApiController api = ApiTestDataFactory.getAdminsApiController();
        Map<String, Metadata> metadata = api.getProjectMetadata(project.getId());

        assertNotNull(metadata, String.format("Fetched %s", objectType));
        assertNotNull(metadata.get(objectKey), String.format("Fetched %s", objectType));
    }

    @Test @Order(40)
    public void deleteProjectMetadataTest() {

        Project project = factory.getFirstProject();
        Long projectId  = project.getId();
        if (project.getMetadata() == null) addProjectMetadataTest();

        AdminsApiController adminApi  = ApiTestDataFactory.getAdminsApiController();
        adminApi.deleteProjectMetadata(projectId, objectItemKey);

        Map<String, Metadata> response = factory.getFirstProject().getMetadata();
        assertNull(response.get(objectItemKey), "Metadata deleted successfully");
    }

    public Metadata getTestObject()
    {
        testObject = testObject
            .or(() -> Optional.ofNullable(factory.getTestProjectMetadataItem(objectItemKey, "test value", true)));

        return testObject.get();
    }

}
