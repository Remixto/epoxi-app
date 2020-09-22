package io.epoxi.engine;

import java.util.List;

public class Config {




    /*
     * PUBSUB CONFIG
    */

    /**The location of the PubSub queues that store messaging for the ETL jobs  */
    public static final String PUBSUB_PROJECT_ID = "etlengine";

    //Where ingestions are queued for processing
    public static final String PUBSUB_SOURCE_TOPIC = "source";

     //The run action on a receiver should timeout after X seconds, if it does not complete before then
     public static final Integer RECEIVER_RUN_TIMEOUT_SECONDS = 300;

     //When reading messages from a Pubsub subscription, what is the default number of messages to attempt to read
     public static final Integer READ_NUM_MESSAGES_ON_PULL_SUBSCRIPTION = 100;

     public static final int PUBSUB_ACK_DEADLINE_SECONDS = 30;

    /*
     * ETL CONFIG
    */

    public static final Integer ETL_EXTRACT_TABLE_EXPIRATION_SECONDS = 2 * 60 * 60; //2 hours
    public static final Integer ETL_TRANSFORM_VIEW_EXPIRATION_SECONDS = 2 * 60 * 60; //2 hours
    public static final Integer ETL_LOAD_TABLE_EXPIRATION_SECONDS = 2 * 60 * 60; //2 hours

    public static final Integer ACTION_BUFFER_DEFAULT_MAX_SIZE = 10000;
    public static final String TARGET_DATASET_NAME = "store";
	public static final String SOURCE_DATASET_NAME = "etl_resource";
    public static final Object SOURCE_VIEW_PREFIX = "source_vw_";

    public static final List<String> RESERVED_FIELD_NAMES = List.of("row_hash");

    /*
     * BEAM PIPELINES
    */
    public static final String PIPELINE_TEMP_DIRECTORY = "gs://temp_etlengine_beam";


}
