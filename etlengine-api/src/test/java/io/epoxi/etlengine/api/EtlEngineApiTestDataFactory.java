package io.epoxi.etlengine.api;

import io.epoxi.repository.App;
import io.epoxi.repository.TestDataFactory;


public class EtlEngineApiTestDataFactory extends TestDataFactory {

    public EtlEngineApiTestDataFactory(String accountName) {
        super(accountName);

    }
    public EtlEngineApiTestDataFactory(Long accountId) {
        super(accountId);
    }

    public static void init() {
        App.init();
    }

    public static EtlEngineApiController getEngineConfigApiController() {
        return new EtlEngineApiController();
    }

    public Long getAccountId()
    {
        return account.getId();
    }

}