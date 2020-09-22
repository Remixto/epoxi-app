package io.epoxi.app.repository.api;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;


public class ApiControllerTestSuite {
   
    @BeforeAll
    public static void setup() {
        clean();
        init();
    }  

    @AfterAll
    public static void tearDown() {

    }

    @Test
    public static void integrationTest() {
        
    }


    private static void clean() {

        //Empty the test datastore
    }

    private static void init() {

    }



}