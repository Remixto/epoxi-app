package io.epoxi.app.repository.api;

import io.epoxi.app.repository.App;
import io.epoxi.app.repository.TestDataFactory;


public class ApiTestDataFactory extends TestDataFactory {

    public ApiTestDataFactory(String accountName) {
        super(accountName);

    }
    public ApiTestDataFactory(Long accountId) {
        super(accountId);
    }

    public static void init() {
        App.init();
    }

    public DevelopersApiController getDevelopersApiController()
    {
        return new DevelopersApiController(getAccountId());
    }

    public static AdminsApiController getAdminsApiController()
    {
        return new AdminsApiController();
    }

    public Long getAccountId()
    {
        return account.getId();
    }

}