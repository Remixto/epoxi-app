package io.epoxi.repository;

import com.googlecode.objectify.ObjectifyService;
import io.epoxi.repository.model.*;
import io.epoxi.repository.modelbase.MemberRepository;
import io.epoxi.cloud.scheduler.Schedule;
import io.epoxi.repository.exception.NotFoundException;
import io.epoxi.repository.modelbase.RefList;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

public class TestDataFactory {

    @Getter
    protected AccountRepository accountRepository;

    @Getter
    protected Account account;
    protected Project project;

    public TestDataFactory(String accountName)
    {
        init();

        try {
            accountRepository = AccountRepository.of(accountName);
            account = accountRepository.get(accountName);
        }
        catch(NotFoundException ex)
        {
            //Create the account because it does not exist
            if (accountName.equals(TestConfig.engineTestAccountName))
            {
                account = createTestAccount(accountName, TestConfig.engineTestProjectName, true);
            }
            else
            {
                account = createTestAccount(accountName, true);
            }

            accountRepository = AccountRepository.of(accountName);
        }

        if (account ==null) throw new RuntimeException("Cannot get or create the specified account instance");
    }

    public TestDataFactory(Long accountId) {

        init();

        accountRepository = AccountRepository.of(accountId);
        account = accountRepository.get(accountId);

        if (account ==null) throw new RuntimeException("Cannot get account instance");
    }

    public Project getFirstProject()
    {
        if (project==null)
        {
            List<Project> projects = accountRepository.getProjectRepository().members();
            if (projects.size() > 0) project = projects.get(0);
        }

        return project;
    }

    /**********************************************
     * Get methods
     *********************************************/

    public MessageQueueRepository getMessageQueueRepository() {
        return getAccountRepository().getMessageQueueRepository();
    }

    public StepEndpointTemplateRepository getStepEndpointTemplateRepository() {
        return getAccountRepository().getStepEndpointTemplateRepository();
    }

    public List<MessageQueue> getMessageQueues() {
        MessageQueueRepository messageQueueRepository = getMessageQueueRepository();
        return messageQueueRepository.members();
    }


    public MessageQueue getTestMessageQueue(String objectName, boolean saveIfCreated) {

        MessageQueue messageQueue =  new MessageQueueRepository().get(objectName);
        if(messageQueue==null) messageQueue = createTestMessageQueue(objectName, saveIfCreated);

        return messageQueue;
    }

    public StepEndpointTemplate getTestStepEndpointTemplate(String objectName, boolean saveIfCreated) {

        StepEndpointTemplate stepEndpointTemplate =  new StepEndpointTemplateRepository().get(objectName);
        if(stepEndpointTemplate==null) stepEndpointTemplate = createTestStepEndpointTemplate(objectName, saveIfCreated);

        return stepEndpointTemplate;
    }

    public static Account getTestAccount(String objectName, boolean saveIfCreated) {
        Account account =  new TestDataFactory(objectName).getAccount();
        if(account==null) account = TestDataFactory.createTestAccount(objectName, String.format("default-project-%s" , objectName), saveIfCreated);

        return account;
    }

    public static Account getTestAccount(String objectName, String defaultFirstProjectName, boolean saveIfCreated) {

        Account account =  new TestDataFactory(objectName).getAccount();
        if(account==null) account = TestDataFactory.createTestAccount(objectName, defaultFirstProjectName, saveIfCreated);

        return account;
    }

    public Project getTestProject(String objectName, boolean saveIfCreated) {
        Project project =  getAccountRepository().getProjectRepository().get(objectName);
        if(project==null) project = createTestProject(objectName, saveIfCreated);

        return project;
    }

    public Ingestion getTestIngestion(KeysetId keysetId) {

        return getAccountRepository().getIngestionRepository().get(keysetId.getKeysetName());
    }

    public Ingestion getTestIngestion(String objectName, Boolean saveIfCreated)
    {
        return getTestIngestion(objectName, true, saveIfCreated);
    }

    public Ingestion getTestIngestion(String objectName, Boolean useAnonymousStreams, Boolean saveIfCreated)
    {
        Ingestion ingestion =  getAccountRepository().getIngestionRepository(getFirstProject().getId()).get(objectName);
        if(ingestion==null) ingestion = createTestIngestion(objectName, saveIfCreated);

        return ingestion;
    }

    public IngestionSync getTestIngestionSync(String ingestionSyncName, String ingestionName, boolean saveIfCreated) {

        return getTestIngestionSync(ingestionSyncName, ingestionName, false, saveIfCreated);
    }

    public IngestionSync getTestIngestionSync(String ingestionSyncName, String ingestionName, Boolean includeSchedule, boolean saveIfCreated) {

        Schedule schedule = null;
        if(Boolean.TRUE.equals(includeSchedule))
            schedule = createTestSchedule("Test Schedule");

        return getTestIngestionSync(ingestionSyncName, ingestionName, schedule, saveIfCreated);
    }

    public IngestionSync getTestIngestionSync(String ingestionSyncName, String ingestionName, Schedule schedule, boolean saveIfCreated) {

        IngestionSync ingestionSync =  getAccountRepository().getIngestionSyncRepository(getFirstProject().getId()).get(ingestionSyncName);
        if(ingestionSync==null) ingestionSync = createTestIngestionSync(ingestionSyncName, ingestionName, schedule, saveIfCreated);

        return ingestionSync;
    }

    public Metadata getTestProjectMetadataItem(String objectKey, String value, boolean saveIfCreated) {

        MemberRepository<Project> projectRepository = getAccountRepository().getEngineProjectRepository();
        Project project = projectRepository.get(getFirstProject().getId());
        Metadata foundMetadata = project.getMetadata().get(objectKey);

        if(foundMetadata==null) foundMetadata = createTestProjectMetadataItem(objectKey, value, saveIfCreated);
        return foundMetadata;
    }

    public Target getTestTarget(String objectName, Boolean saveIfCreated)
    {
        Target target = getAccountRepository().getTargetRepository().get(objectName);
        if(target==null) target = createTestTarget(objectName, saveIfCreated);

        return target;
    }

    public Source getTestSource(String objectName, Boolean saveIfCreated)
    {
        Source source = getAccountRepository().getSourceRepository().get(objectName);
        if(source==null) source = createTestSource(objectName, saveIfCreated);

        return source;
    }

    public Stream getTestSourceStream(String objectName, String sourceName, Boolean saveIfCreated)
    {
        Stream stream = getAccountRepository().getStreamRepository().get(objectName);
        if(stream==null) stream = createTestSourceStream(objectName, sourceName, saveIfCreated);

        return stream;
    }

    public Stream getTestFKStream(String objectName, String sourceName, String sourceFieldPrefix, Boolean saveIfCreated)
    {

        Stream stream = getAccountRepository().getStreamRepository().get(objectName);
        if(stream==null) stream = createTestFKStream(objectName, sourceName, sourceFieldPrefix, saveIfCreated);

        return stream;
    }


    /***********************************************
     * Create methods
     **********************************************/

    public Ingestion createTestIngestion(String name)
    {
        return createTestIngestion(name, false);
    }

    /**
     * Create test ingestion using fake (and meaningless) test data
     * @param name The name of the ingestion to return
     * @param save If true, the ingestion  (and all its child components) will be saved before being returned. If false, the child components will be saved.
     * @return An ingestion
     */
    public Ingestion createTestIngestion(String name, Boolean save)
    {
        return createTestIngestion(name, true, save);
    }

    /**
     * Create test ingestion using fake (and meaningless) test data
     * @param name The name of the ingestion to return
     * @param useAnonymousStreams If true, streams that make up the ingestion will be unnamed
     * @param save If true, the ingestion (and all its child components) will be saved before being returned.  If false, the child components will be saved.
     * @return An ingestion
     */
    public Ingestion createTestIngestion(String name, Boolean useAnonymousStreams, Boolean save)
    {
        Ingestion ingestion;
        Target target = getTestTarget(String.format("Test Target for %s", name), true);

        //Get two streams
        RefList<Stream> streams = new RefList<>();

        String sourceName = AppendName(name, "Source");
        String fkSourceName = AppendName(name, "fkSource");

        if(useAnonymousStreams)
        {
            Stream stream = createTestSourceStream(sourceName, true);
            streams.add(stream.toRef());

            //Make a secondary streams
            streams.add(createTestFKStream(fkSourceName, fkSourceName, true).toRef());
        }
        else
        {
            String sourceStreamName = AppendName(name, "SourceStream");
            Stream stream = getTestSourceStream(sourceStreamName, sourceName, true);

            streams.add(stream.toRef());

            //Make a secondary streams
            String fkStreamName = AppendName(name, "FKStream");
            streams.add(getTestFKStream(fkStreamName, fkSourceName, fkSourceName, true).toRef());
        }

        //Create the ingestion from the assembled parts
        ingestion = createTestIngestion(name, streams.toIdList(), target.getId(), save);

        return ingestion;
    }

    /**
     * Create test ingestion preexisting streams and target test data
     * @param name The name of the ingestion to return
     * @param streamIds The list of streams that will be set in the ingestion
     * @param targetId The id of the target to be set in the ingestion
     * @param save If true, the ingestion (and all its child components) will be saved before being returned.  If false, the child components will be saved.
     * @return An ingestion
     */
    public Ingestion createTestIngestion(String name, List<Long> streamIds, Long targetId, Boolean save)
    {
        return createTestIngestion(name, streamIds, targetId, Ingestion.ScdType.TYPE_I, Ingestion.ReplicationType.FULL, save);

    }

    /**
     * Create test ingestion preexisting streams and target test data
     * @param name The name of the ingestion to return
     * @param streamIds The list of streams that will be set in the ingestion
     * @param targetId The id of the target to be set in the ingestion
     * @param scdType The scdType of the new ingestion
     * @param replicationType The ReplicationType of the new ingestion
     * @param save If true, the ingestion (and all its child components) will be saved before being returned.  If false, the child components will be saved.
     * @return An ingestion
     */
    public Ingestion createTestIngestion(String name, List<Long> streamIds, Long targetId, Ingestion.ScdType scdType, Ingestion.ReplicationType replicationType, Boolean save)
    {
        Project project = getFirstProject();

        Ingestion ingestion = Ingestion.newBuilder()
                .withName(name).fromStreams(streamIds).toTarget(targetId)
                .setProject(project)
                .setWeight(100)
                .setScdType(scdType)
                .setUpdateType(replicationType)
                .build();

        if (save)
        {
            IngestionRepository repository = getAccountRepository().getIngestionRepository(ingestion.getProjectId());
            repository.add(ingestion);
        }

        return ingestion;
    }

    private IngestionSync createTestIngestionSync(String ingestionSyncName, String ingestionName, Schedule schedule, boolean save) {

        Ingestion ingestion = getTestIngestion(ingestionName, true);
        IngestionSync.OptionalsBuilder builder = IngestionSync.newBuilder()
                .withName(ingestionSyncName)
                .syncIngestion(ingestion.getId());

        if (schedule !=null) builder.setSchedule(schedule);
        IngestionSync obj = builder.build();


        if (save)
        {
            IngestionSyncRepository repository = getAccountRepository().getIngestionSyncRepository(ingestion.getProjectId());
            repository.add(obj);
        }

        return obj;
    }



    public Project createTestProject(String objectName)
    {
        return createTestProject(objectName, false);
    }

    public Project createTestProject(String objectName, Boolean save)
    {
        Project obj = Project.of(getAccountId(), objectName, Project.TargetType.BIGQUERY);

        if (save)
        {
            ProjectRepository repository = getAccountRepository().getProjectRepository();
            repository.add(obj);
        }

        return obj;
    }

    public Source createTestSource(String objectName)
    {
        return createTestSource(objectName, false);
    }

    public Source createTestSource(String objectName, Boolean save)
    {
        SourceType sourceType = SourceType.of("BQ");
        return createTestSource(sourceType, objectName, save);
    }

    public Source createTestSource(SourceType sourceType, String objectName)
    {
        return createTestSource(sourceType, objectName, false);
    }

    public Source createTestSource(SourceType sourceType, String objectName, Boolean save)
    {
        Source obj = Source.newBuilder()
                .withName(objectName)
                .withSourceType(sourceType)
                .setProject(getFirstProject())
                .build();

        if (save)
        {
            MemberRepository<Source> repository = getAccountRepository().getSourceRepository(obj.getProjectId());
            repository.add(obj);
        }
        return obj;
    }

    public Target createTestTarget(String objectName)
    {
        return createTestTarget(objectName, false);
    }

    public Target createTestTarget(String objectName, Boolean save)
    {
        //Create
        Target obj = Target.newBuilder()
                .withName(objectName)
                .setProject(getFirstProject())
                .build();

        if (save) {
            MemberRepository<Target> repository = getAccountRepository().getTargetRepository(obj.getProjectId());
            repository.add(obj);
        }

        return obj;
    }

    public Stream createTestBaseStream(String sourceName, String keyFieldId, Boolean save)
    {
        return createTestStream(null, sourceName, null, keyFieldId, null, save);
    }

    public Stream createTestFKStream(String sourceName, String sourceFieldPrefix)
    {
        return createTestFKStream(null, sourceName, sourceFieldPrefix);
    }

    public Stream createTestFKStream(String sourceName, String sourceFieldPrefix, Boolean save)
    {
        return createTestFKStream(null, sourceName, sourceFieldPrefix, save);
    }

    public Stream createTestFKStream(String streamName, String sourceName, String sourceFieldPrefix)
    {
        return createTestFKStream(streamName, sourceName, sourceFieldPrefix, false);
    }

    public Stream createTestFKStream(String streamName, String sourceName, String sourceFieldPrefix, Boolean save)
    {
        String keyFieldId = String.format("%s_Key", sourceFieldPrefix);
        List<Field> fields = createTestFKStreamFields(sourceFieldPrefix);

        return createTestStream(streamName, sourceName, fields, keyFieldId, sourceFieldPrefix, save);
    }

    public Stream createTestSourceStream(String sourceName)
    {
        return createTestSourceStream(sourceName, false);
    }

    public Stream createTestSourceStream(String sourceName, Boolean save)
    {
        return createTestSourceStream(null, sourceName, save);
    }

    public Stream createTestSourceStream(String name, String sourceName, Boolean save)
    {
        return createTestStream(name, sourceName, null, sourceName, null, save);
    }

    public Stream createTestStream(String name, String sourceName, List<Field> fields)
    {
        return createTestStream(name, sourceName, fields, sourceName, null, false);
    }

    public Stream createTestStream(String name, String sourceName, List<Field> fields, String hashKeyFieldId, String nestFieldId, Boolean save)
    {
        Source source = getTestSource(sourceName, true);
        Schema schema = null;
        if (fields!=null && !fields.isEmpty()) schema = Schema.newBuilder().setFields(fields).build();

        Stream.OptionalsBuilder builder;
        if (name!=null)
        {
            builder = Stream.newNamedBuilder().withName(name).fromSource(source.getId());
        }
        else
        {
            builder = Stream.newAnonymousBuilder().fromSource(source.getId());
        }

        Stream obj = builder.setProject(getFirstProject()).setSchema(schema).setHashKeyFieldId(hashKeyFieldId).setNestFieldId(nestFieldId).build();

        if (save)
        {
            MemberRepository<Stream> repository = getAccountRepository().getStreamRepository(obj.getProjectId());
            repository.add(obj);
        }

        return obj;
    }

    /**
     * //Create the stream fields by default for fields where the suffix is Key, Code and Desc
     */
    public List<Field> createTestFKStreamFields(String name) {

        List<String> suffixList = List.of("Key", "Code", "Desc"); //Make sure the key field is in the list

        return createTestFKStreamFields(name, suffixList);
    }

    public List<Field> createTestFKStreamFields(String name, List<String> suffixList) {

        List<Field> fields = new ArrayList<>();

        for(String suffix : suffixList)
        {
            fields.add( Field.of(suffix, String.format("%s_%s", name, suffix)));
        }

        return fields;
    }

    public Metadata createTestProjectMetadataItem(String key, String value)
    {
        return createTestProjectMetadataItem(key, value, false);
    }

    public Metadata createTestProjectMetadataItem(String key, String value, Boolean save)
    {
        //Create
        Metadata obj = Metadata.of(key, value);

        if (save)
        {

            MemberRepository<Project> projectRepository = getAccountRepository().getEngineProjectRepository();
            Project project = projectRepository.get(getFirstProject().getId());
            project.getMetadata().put(key, obj);

            projectRepository.add(project);
        }

        return obj;
    }

    public Schedule createTestSchedule(String objectName) {
        return Schedule.newBuilder(TestConfig.CLOUD_SCHEDULER_PROJECT_ID, TestConfig.CLOUD_SCHEDULER_LOCATION_ID)
                .setName(objectName)
                .setDetail("*", "*", "1", "*", "*")
                .build();

    }

    public static StepEndpointTemplate createTestStepEndpointTemplate(String objectName)
    {

        return createTestStepEndpointTemplate(objectName, false);
    }

    public static StepEndpointTemplate createTestStepEndpointTemplate(String objectName, Boolean save)
    {
        //Create
        String tableId = "{projectName}.temp.etl_load_{keysetName}";
        String keyFieldId = "{stepEndpointTemplate.tableName}_Key";

        StepEndpointTemplate obj = StepEndpointTemplate.newBuilder()
                .withName(objectName)
                .withEndpointType(StepEndpointTemplate.EndpointType.TARGET)
                .withStepType(StepType.TRANSFORM)
                .withTableId(tableId)
                .setKeyFieldId(keyFieldId)
                .build();

        if (save)
        {
            StepEndpointTemplateRepository repository = new StepEndpointTemplateRepository();
            repository.add(obj);
        }

        return obj;
    }

    public static MessageQueue createTestMessageQueue(String objectName)
    {
        return createTestMessageQueue(objectName, false);
    }

    public static MessageQueue createTestMessageQueue(String objectName, Boolean save)
    {
        //Create
        MessageQueue obj = MessageQueue.newBuilder()
                .withName(objectName)
                .withQueueType(MessageQueue.QueueType.IN)
                .withStepType(StepType.LOAD)
                .withQueuePath("projects/etlengine/subscriptions/etl_transform_reader")
                .build();

        if (save)
        {
            MessageQueueRepository repository = new MessageQueueRepository();
            repository.add(obj);
        }

        return obj;
    }

    private String AppendName(String existing, String additional)
    {
        return String.format("%s/%s", existing, additional);
    }

    public static Account createTestAccount(String objectName)
    {
        return createTestAccount(objectName, false);
    }

    public static Account createTestAccount(String objectName, Boolean save)
    {
        return createTestAccount(objectName, String.format("default-project-%s", objectName), save);
    }

    public static Account createTestAccount(String objectName, String firstProjectName)
    {
        return createTestAccount(objectName, firstProjectName, false);
    }

    public static Account createTestAccount(String objectName, String firstProjectName, Boolean save)
    {
        init();

        Account obj = Account.newBuilder()
                .withAccountName(objectName)
                .withFirstName("Jessie")
                .withLastName("James")
                .setCompany("Test Company")
                .build();

        if (save)
        {
            //Save the new account
            AccountRepository accountRepository = new AccountRepository();
            accountRepository.add(obj);

            //Add a first project
            accountRepository = AccountRepository.of(objectName);
            Project project = Project.newBuilder()
                    .withName(firstProjectName)
                    .withAccountId(obj.getId())
                    .withTargetType(Project.TargetType.BIGQUERY)
                    .build();

            accountRepository.getProjectRepository().add(project);
        }

        return obj;
    }



    public static void init()
    {
        if (Boolean.TRUE.equals(initialized)) return;  //Tests use this class.  Multiple unit tests will call this repeatably so lets leave early if the job is done.

        ObjectifyService.init();
        initialized = true;
    }

    static Boolean initialized = false;


    /************************************************
     * Helper methods
     */

    public Long getAccountId()
    {
        return account.getId();
    }


}
