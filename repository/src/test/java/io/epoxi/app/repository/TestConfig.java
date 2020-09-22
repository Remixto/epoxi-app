package io.epoxi.app.repository;

public class TestConfig {

    public final static String utilTestProjectId = "warehouse-277017";


    /********************************************
     * DATASTORE CONFIG
     ********************************************/
    public final static String engineTestAccountName = "engineTest-Account";
    public final static String etlTestAccountName = "engineTest-Account";
    public final static String apiTestAccountName = "apiTest-Account";
    public static final String engineTestProjectName = "warehouse-277017";

    /********************************************
     * CLOUD_SCHEDULER CONFIG
     ********************************************/
    public static final String CLOUD_SCHEDULER_PROJECT_ID = "etlengine";
    public static final String CLOUD_SCHEDULER_LOCATION_ID = "us-east1";

    /********************************************
     * PUBSUB CONFIG
     ********************************************/
    public static final String PUBSUB_PROJECT_ID = "etlengine";

    public static final String PIPELINE_TEMP_DIRECTORY = "gs://temp_etlengine_beam";
}