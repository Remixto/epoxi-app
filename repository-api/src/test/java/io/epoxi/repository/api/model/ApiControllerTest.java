package io.epoxi.repository.api.model;

import com.googlecode.objectify.NotFoundException;
import io.epoxi.repository.AccountRepository;
import io.epoxi.repository.TestConfig;
import io.epoxi.repository.api.ApiTestDataFactory;
import io.epoxi.repository.api.DevelopersApiController;
import io.epoxi.repository.model.Account;
import io.epoxi.repository.api.AdminsApiController;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ApiControllerTest {

    @BeforeAll
    public static void setUp() {
        ApiTestDataFactory.init();
    }

    @Test
    public void createAdminsApiTest() {

        //Create
        AdminsApiController api = new AdminsApiController();
        assertNotNull(api);
    }

    @Test
    public void createDevelopersApiTest() {

        AccountRepository repository;
        //Create
        try
        {
            repository = AccountRepository.of(TestConfig.apiTestAccountName);
        }
        catch(NotFoundException ex)
        {
            AdminsApiController adminsApi = new AdminsApiController();
            String firstName = "Karen";
            String lastName = "Draper";

            adminsApi.addAccount(   Account.newBuilder()
                                                .withAccountName(TestConfig.apiTestAccountName)
                                                .withFirstName(firstName)
                                                .withLastName(lastName)
                                                .build());

            repository = AccountRepository.of(TestConfig.apiTestAccountName);
        }

        DevelopersApiController api = new DevelopersApiController(repository.getAccount().getId());

        assertNotNull(api);
    }

}
