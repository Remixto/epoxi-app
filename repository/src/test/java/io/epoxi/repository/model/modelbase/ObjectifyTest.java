package io.epoxi.repository.model.modelbase;

import com.googlecode.objectify.ObjectifyFactory;
import com.googlecode.objectify.ObjectifyService;
import io.epoxi.repository.AccountRepository;
import io.epoxi.repository.TestConfig;
import io.epoxi.repository.TestDataFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ObjectifyTest {

    static TestDataFactory factory;

    @BeforeAll
    public static void setUp() {
        //Setting the factory will initialize the ObjectifyFactory
        factory = new TestDataFactory(TestConfig.apiTestAccountName);
    }

    @Test
    public void ObjectifyRegistryInitTest() {
        ObjectifyFactory factory = ObjectifyService.factory();
        assertNotNull(factory, "Objectify Registry successfully registered objects");
    }

    @Test @Disabled
    public void purgeTestDatastoreAccountsTest() {

        AccountRepository accountRepository = factory.getAccountRepository();
        assertDoesNotThrow((Executable) accountRepository::purge);
    }
}
