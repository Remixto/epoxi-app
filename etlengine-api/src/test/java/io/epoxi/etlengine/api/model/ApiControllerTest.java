package io.epoxi.etlengine.api.model;

import io.epoxi.etlengine.api.EtlEngineApiController;
import io.epoxi.etlengine.api.EtlEngineApiTestDataFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ApiControllerTest {

    @BeforeAll
    public static void setUp() {
        EtlEngineApiTestDataFactory.init();
    }

    @Test
    public void createEtlEngineApiTest() {

        //Create
        EtlEngineApiController api = new EtlEngineApiController();
        assertNotNull(api);
    }

}
