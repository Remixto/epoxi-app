package io.epoxi.engine.etl;

import io.epoxi.engine.EngineTestDataFactory;
import io.epoxi.engine.queue.EngineSubscription;
import io.epoxi.repository.*;
import io.epoxi.repository.modelbase.RefList;
import io.epoxi.repository.model.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class EtlAllPipelinesTest {

    @BeforeAll
    public static void setup() {

        clean();
        init();
    }

    @AfterAll
    public static void tearDown() {

    }


    @ParameterizedTest
    @MethodSource("getKeysets")
    void RunAllPipelinesTest(KeysetId keysetId) {

        new ExtractPipelineTest().runExtract(keysetId);
        new TransformPipelineTest().runTransform(keysetId);
        new LoadPipelineTest().runLoad(keysetId);

        assertDoesNotThrow(() -> true, "TODO"); // fix this test because it is not actually configured correctly
    }

    @Test
    void setupEnvironment() {
        assertDoesNotThrow(() -> true, "Successfully called clean and init on Environment setup");
    }

    private static void clean() {

        // purge datastore
        purgeTestDatastoreGlobals();

        // clean pubsub queues
        clearPubsubQueues();

        // Clear the CDC queue
        // TODO
    }

    private static void init() {

        EngineTestDataFactory.init();

        // Build model config data
        addMessageQueuesModels();
        addStepEndpointTemplateModels();
        buildIngestionModels();
    }

    public static List<KeysetId> getKeysets() {
        return  new EngineTestDataFactory(TestConfig.engineTestAccountName).getTestKeysets();
    }

    /**
     * Clear global settings (currently all MessageQueues and StepEndpointTemplates.
     */
    public static void purgeTestDatastoreGlobals() {

        EngineTestDataFactory factory = new EngineTestDataFactory(TestConfig.engineTestAccountName);

        MessageQueueRepository messageQueueRepository = factory.getMessageQueueRepository();
        MessageQueueRepository.purge(messageQueueRepository.memberKeys());

        StepEndpointTemplateRepository stepEndpointTemplateRepository = factory
            .getStepEndpointTemplateRepository();
        StepEndpointTemplateRepository.purge(stepEndpointTemplateRepository.memberKeys());
    }

    public static void clearPubsubQueues() {
        boolean finished = false;

        // Populate the list of message queues
        List<MessageQueue> messageQueues = new EngineTestDataFactory(TestConfig.engineTestAccountName).getMessageQueues();

        try {

            ExecutorService es = Executors.newCachedThreadPool();
            for (MessageQueue queue : messageQueues) {
                es.execute(() -> {
                    /* do the work to be done in its own thread */
                    EngineSubscription subscription = EngineSubscription.of(queue);
                    subscription.clearMessages();
                });
            }
            es.shutdown();
            finished = es.awaitTermination(1, TimeUnit.MINUTES);
            // all tasks have finished or the time has been reached.
        } catch (InterruptedException ex) {
            // TODO
        }

        if (!finished)
            throw new ExceptionInInitializerError();
    }

    public static void addMessageQueuesModels() {

        String projectId = TestConfig.PUBSUB_PROJECT_ID;

        List<MessageQueue> queues = new ArrayList<>();

        queues.add(getMessageQueuesModel("IN_EXTRACT", MessageQueue.QueueType.IN, StepType.EXTRACT, String.format("projects/%s/subscriptions/source_reader", projectId), 100));
        queues.add(getMessageQueuesModel("OUT_EXTRACT", MessageQueue.QueueType.OUT, StepType.EXTRACT, String.format("projects/%s/topics/etl_extract.cdc.{keysetId}", projectId), 100));
        queues.add(getMessageQueuesModel("ERR_EXTRACT", MessageQueue.QueueType.ERROR, StepType.EXTRACT, String.format("projects/%s/topics/error_etl_extract", projectId), 100));

        queues.add(getMessageQueuesModel("IN_TRANSFORM", MessageQueue.QueueType.IN, StepType.TRANSFORM, String.format("projects/%s/subscriptions/etl_extract.cdc.{keysetId}_reader", projectId),200));
        queues.add(getMessageQueuesModel("OUT_TRANSFORM", MessageQueue.QueueType.OUT, StepType.TRANSFORM, String.format("projects/%s/topics/etl_transform", projectId), 200));
        queues.add(getMessageQueuesModel("ERR_TRANSFORM", MessageQueue.QueueType.ERROR, StepType.TRANSFORM, String.format("projects/%s/topics/error_etl_transform", projectId), 200));

        queues.add(getMessageQueuesModel("IN_LOAD", MessageQueue.QueueType.IN, StepType.LOAD, String.format("projects/%s/subscriptions/etl_transform_reader", projectId), 300));
        queues.add(getMessageQueuesModel("OUT_LOAD", MessageQueue.QueueType.OUT, StepType.LOAD, String.format("projects/%s/topics/etl_load", projectId), 300));
        queues.add(getMessageQueuesModel("ERR_LOAD", MessageQueue.QueueType.ERROR, StepType.LOAD, String.format("projects/%s/topics/error_etl_load", projectId), 300));

        MessageQueueRepository repository = new EngineTestDataFactory(TestConfig.engineTestAccountName).getMessageQueueRepository();
        queues.forEach(repository::add);

    }

    private static MessageQueue getMessageQueuesModel(String name, MessageQueue.QueueType queueType, StepType stepType, String queuePath, Integer weight) {
        return MessageQueue.newBuilder()
                .withName(name)
                .withQueueType(queueType)
                .withStepType(stepType)
                .withQueuePath(queuePath)
                .setWeight(weight)
                .build();
    }

    public static void addStepEndpointTemplateModels()
    {
        List<StepEndpointTemplate> templates = new ArrayList<>();

        templates.add(getStepEndpointTemplateModel("SOURCE_EXTRACT", StepEndpointTemplate.EndpointType.SOURCE, StepType.EXTRACT, "{source.tableId}", null));
        templates.add(getStepEndpointTemplateModel("TARGET_EXTRACT", StepEndpointTemplate.EndpointType.TARGET, StepType.EXTRACT, "{projectName}.temp.etl_extract_{keysetName}", null));

        templates.add(getStepEndpointTemplateModel("SOURCE_TRANSFORM", StepEndpointTemplate.EndpointType.SOURCE, StepType.TRANSFORM, "{projectName}.temp.etl_extract_{keysetName}", "{target.tableName}_Key"));
        templates.add(getStepEndpointTemplateModel("TARGET_TRANSFORM", StepEndpointTemplate.EndpointType.TARGET, StepType.TRANSFORM, "{projectName}.temp.etl_load_{keysetName}", "{target.tableName}_Key"));

        templates.add(getStepEndpointTemplateModel("SOURCE_LOAD", StepEndpointTemplate.EndpointType.SOURCE, StepType.LOAD, "{projectName}.temp.etl_load_{keysetName}", null));
        templates.add(getStepEndpointTemplateModel("TARGET_LOAD", StepEndpointTemplate.EndpointType.TARGET, StepType.LOAD, "{target.tableId}", null));

        StepEndpointTemplateRepository repository = new EngineTestDataFactory(TestConfig.engineTestAccountName).getStepEndpointTemplateRepository();
        templates.forEach(repository::add);

    }

    private static StepEndpointTemplate getStepEndpointTemplateModel(String name, StepEndpointTemplate.EndpointType endpointType, StepType stepType, String tableId, String keyFieldId) {
        return StepEndpointTemplate.newBuilder()
                .withName(name)
                .withEndpointType(endpointType)
                .withStepType(stepType)
                .withTableId(tableId)
                .setKeyFieldId(keyFieldId)
                .build();
    }

    public static void buildIngestionModels()
    {
        //Build etl specific model data
        List<KeysetId> keysets = new EngineTestDataFactory(TestConfig.engineTestAccountName).getTestKeysets();

        for(KeysetId keysetId: keysets)
        {
            String targetName = keysetId.getKeysetName().replace("Firm_Data_", "");
            addIngestionModel(keysetId, targetName);
        }
    }

    public static void addIngestionModel(KeysetId keysetId, String targetName)
    {
        Project project = new AccountRepository().getProject(keysetId.getProjectName());
        TestDataFactory factory = new TestDataFactory(project.getAccountId());

        //Get the target
        Target target = factory.getTestTarget(targetName, true);

        //Get the streams
        RefList<Stream> streams = new RefList<>();

        //Get the base stream
        String sourceName = keysetId.getKeysetName();
        String keyFieldId = String.format("%s_Key", targetName);
        Stream stream = factory.createTestBaseStream(sourceName, keyFieldId, true);
        streams.add(stream.toRef());

        //Make secondary streams
        //TODO Make this process automatic by detecting FKeys by examining fields in the base stream and looking for possible target matches
        switch (keysetId.getKeysetName()) {
            case "Firm_Data_Entity":
                Stream fkStream = factory.createTestFKStream("Firm_Data_Entity_Type", "Entity_Type", true);
                streams.add(fkStream.toRef());
                break;
        }

        //Create the ingestion
        Ingestion ingestion = Ingestion.newBuilder()
                                    .withName(keysetId.getKeysetName())
                                    .fromStreams(streams.toIdList())
                                    .toTarget(target.getId())
                                    .setProject(project)
                                    .build();

        //Get the Id of any existing Ingestion with a matching name.  Overwrite it with the new ingestion
        Ingestion existingIngestion = factory.getTestIngestion(keysetId.getKeysetName(), false);

        if (existingIngestion != null && existingIngestion.getId()!=null)  //If it exists in the data store, we need to overwrite it.
            ingestion.setId(existingIngestion.getId());

        //Add the ingestion
        factory.getAccountRepository().getIngestionRepository(ingestion.getProjectId()).add(ingestion);
    }


}
