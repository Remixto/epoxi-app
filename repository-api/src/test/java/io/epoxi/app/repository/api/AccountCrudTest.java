package io.epoxi.app.repository.api;

import io.epoxi.app.repository.model.Account;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AccountCrudTest {

    static ApiTestDataFactory factory;
    static private Optional<Account> testObject = Optional.empty();
    static private String objectName;
    static private String objectType;

    @BeforeAll
    public static void setUp() {

        ApiTestDataFactory.init();
        String accountName ="adminApiTest-Account";
        factory = new ApiTestDataFactory(accountName);

        objectType = Account.class.getSimpleName();
        objectName = accountName;
    }

    @Test @Order(1)
    void addAccountTest() {

        //If an existing item exists, delete it
        AdminsApiController api  = ApiTestDataFactory.getAdminsApiController();
        Account existingObj = api.getAccount(objectName);
        if (existingObj !=null) api.deleteAccount(existingObj.getId(), true);

        //Create
        Account obj = ApiTestDataFactory.createTestAccount(objectName, true);

        //Assert
        assertNotNull(obj.getId().toString(), String.format("Fetched %s with the expected name", objectType));
    }

    @Test  @Order(2)
    void getAdminAccountTest() {

        Long objectId = getTestObject().getId();
        AdminsApiController adminApi  = ApiTestDataFactory.getAdminsApiController();

        //Get by ID
        Account response = adminApi.getAccount(objectId);

        assertNotNull(response, String.format("Fetched %s", objectType));
        assertEquals(objectId, response.getId(), String.format("Fetched %s with the expected Id", objectType));
        assertNotNull(response.getName(), String.format("Fetched %s with a valid name", objectType));

        //Get by name
        String name = response.getName();

        response = adminApi.getAccount(name);
        assertNotNull(response, String.format("Fetched %s", objectType));
        assertEquals(objectId, response.getId(), String.format("Fetched %s with the expected Id", objectType));
        assertNotNull(response.getName(), String.format("Fetched %s with a valid name", objectType));
    }

    @Test  @Order(3)
    void getDevAccountTest() {

        //Ensure we have a dev account
        String accountName ="devApiTest-Account";
        ApiTestDataFactory.getTestAccount(accountName, true);

        //Test
        ApiTestDataFactory devFactory = new ApiTestDataFactory(accountName);
        DevelopersApiController devApi  = devFactory.getDevelopersApiController();
        Account response = devApi.getAccount();

        assertNotNull(response, String.format("Fetched %s", objectType));
        assertNotNull(response.getName(), String.format("Fetched %s with a valid name", objectType));
    }

    @Test @Order(4)
    void searchAccountTest() {

        //Search by name
        assertNotNull(getTestObject(), String.format("%s searched successfully for a specific name", objectType));

        AdminsApiController adminsApi  = ApiTestDataFactory.getAdminsApiController();

        //Search by pattern
        List<Account> response = adminsApi.searchAccount(objectName.substring(2), null, 0, 1);
        assertNotNull(response, String.format("%s searched successfully", objectType));
        assertNotNull(response.get(0).getName(), String.format("%s searched successfully with matching pattern", objectType));

    }

    @Test @Order(5)
    void deleteAccountTest() {

        testObject = Optional.empty(); //reset the test object
        assertDoesNotThrow(() -> deleteAccount(false), "Soft delete executed successfully for Account object");

        testObject = Optional.empty(); //reset the test object
        assertDoesNotThrow(() -> deleteAccount(true), "Hard delete executed successfully for Account object");
    }

    void deleteAccount(Boolean permanent) {

        AdminsApiController adminApi  = ApiTestDataFactory.getAdminsApiController();
        Long objectId = getTestObject().getId();
        adminApi.deleteAccount(objectId, permanent);

        assertNull(adminApi.getAccount(objectId), String.format("%s deleted successfully (permanent = %s)", objectType, permanent));
    }

    Account getTestObject()
    {
        testObject = testObject
        .or(() -> Optional.ofNullable(ApiTestDataFactory.getTestAccount(objectName, true)));

        return testObject.get();
    }
}
