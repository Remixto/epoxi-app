package io.epoxi.app.repository;

public class Config {

    private Config()
    {}

    /*
     * CLOUD_SCHEDULER CONFIG
     */
    public static final String CLOUD_SCHEDULER_PROJECT_ID = "etlengine";
    public static final String CLOUD_SCHEDULER_LOCATION_ID = "us-east1";

    /*
     * PUBSUB CONFIG
    */
    /**The location of the PubSub queues that store messaging for the ETL jobs  */
    public static final String PUBSUB_PROJECT_ID = "etlengine";

    //Where ingestions are queued for processing
    public static final String PUBSUB_SOURCE_TOPIC = "source";

    /*
     * ETL CONFIG
     */
    public static final String TARGET_DATASET_NAME = "store";
    public static final String SOURCE_DATASET_NAME = "etl_resource";
    public static final Object SOURCE_VIEW_PREFIX = "source_vw_";
}
