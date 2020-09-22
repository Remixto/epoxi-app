package io.epoxi.app.repository.model;

import io.epoxi.app.cloud.scheduler.JobScheduler;
import io.epoxi.app.repository.AccountRepository;
import io.epoxi.app.repository.modelbase.EngineMember;
import io.epoxi.app.repository.modelbase.ProjectMember;
import io.swagger.annotations.ApiModel;

import io.swagger.annotations.ApiModelProperty;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import io.epoxi.app.repository.IngestionSyncRepository;
import io.epoxi.app.cloud.scheduler.Schedule;
import io.epoxi.app.repository.Config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.scheduler.v1.Job;
import com.google.cloud.scheduler.v1.PubsubTarget;
import com.google.cloud.scheduler.v1.Job.State;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import com.googlecode.objectify.Ref;
import com.googlecode.objectify.annotation.Entity;

/**
 * executes an ingestion on a specific schedule. If not schedule is specified,
 * the execution is immediate.
 **/
@ApiModel(description = "executes an ingestion on a specific schedule.  If not schedule is specified, the execution is immediate.")
@Entity
public class IngestionSync extends ProjectMember<IngestionSync> {

    private IngestionSync() {
    }

    private IngestionSync(String name, Long ingestionId) {

        this.name = name;      
        setIngestion(ingestionId);  
        setProject(getIngestion().getProjectId());
    }

    @ApiModelProperty(required = true, value = "the ingestion to be scheduled")
    @NonNull
    /*"the ingestion to be scheduled"
     * 
     */
    private Ref<Ingestion> ingestion = null;

    @ApiModelProperty(value = "the schedule that determines when the ingestion should be triggered")
    @Getter(onMethod=@__({@JsonProperty("schedule")}))
    @Setter
    /*
     * "the schedule that determines when the ingestion should be triggered"
     */
    private Schedule schedule = null;

    @ApiModelProperty(value = "sets a state that indicates whether the ingestion can be synced.  When this flag is anything other than Active, ingestion syncs will not occur")
    @NonNull
    @Getter(onMethod=@__({@JsonProperty("state")}))
    @Setter
    /*
     * sets a flag that indicates whether the ingestion can be synced. When this flag is anything other than Active, ingestion syncs will not occur
     */
    private SyncState state = SyncState.UNSPECIFIED;

    /*
     * Get source
     * 
     * @return source
     */
    @JsonProperty("ingestion")
    public Ingestion getIngestion()
    { 
        return ingestion.get();
    }

    public void setIngestion(@NonNull Ingestion ingestion) {
    
        setIngestion(ingestion.getId());     
     
    }

    @JsonIgnore
    public void setIngestion(@NonNull Long ingestionId) {      
        this.ingestion = Ingestion.createRef(ingestionId);
    }
   
    @Override
    public String toString() {
        return EngineMember.toString(this);
    }

    @Override
    @JsonIgnore
    public IngestionSyncRepository getRepository() {
        AccountRepository accountRepository = AccountRepository.of(getAccountId());
        return accountRepository.getIngestionSyncRepository() ;
    }

    @JsonIgnore
    public Job toScheduledJob()
    {
      State jobState = getJobState(getState());          

      //If the job exists, delete it
      JobScheduler jobScheduler = schedule.getJobScheduler();
      jobScheduler.delete();      

      PubsubMessage message = getIngestion().toMessage();

      // Other options are to create an App Engine HTTP target or a PubSub target.
      PubsubTarget pubsubTarget = PubsubTarget.newBuilder()          
          .setTopicName(getIngestionTopic().toString())
          .setData(message.getData())
          .build();

      return Job.newBuilder()
          .setName(schedule.getJobName().toString())
          .setDescription(String.format("Schedule for Ingestion '%s' (AccountId=%s)", getName(), getAccountId()))
          .setSchedule(schedule.getDetail())
          .setTimeZone(schedule.getTimezone())
          .setState(jobState)
          .setPubsubTarget(pubsubTarget)
          .build();      
    }

    public static TopicName getIngestionTopic()
    {
      return TopicName.of(Config.PUBSUB_PROJECT_ID, Config.PUBSUB_SOURCE_TOPIC);      
    }

    private State getJobState(SyncState ingestionState)
    {
      State jobState = null;

      switch(ingestionState)
          {
              case UNSPECIFIED:
                jobState = State.STATE_UNSPECIFIED;
                break;
              case ENABLED:
                jobState = State.ENABLED;
                break;
              case PAUSED:
                jobState = State.PAUSED;
                break;
              case FAILED:
                jobState = State.UPDATE_FAILED;
                break;
          }

          return jobState;
    }

    public enum SyncState{
        UNSPECIFIED,
        ENABLED,
        PAUSED,
        FAILED
    }

    public static Builder newBuilder() { 
       
        return name -> ingestionId -> new OptionalsBuilder().setName(name).setIngestionId(ingestionId);       
    }

    
    /******************************************
     * Builder.  A functional interface with a single method (withName) that starts the build.
     */

    @FunctionalInterface
    public interface Builder {

        IngestionBuilder withName(final String name);

        interface IngestionBuilder
        {
            OptionalsBuilder syncIngestion(final Long ingestionId);
        }
    }
   
    public static class OptionalsBuilder
    {   
        @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) String name;        
        @Accessors(chain = true) @NonNull @Setter(AccessLevel.PRIVATE) Long ingestionId;

        @Accessors(chain = true) @NonNull @Setter() Schedule schedule;

        public IngestionSync build(){

            IngestionSync obj = new IngestionSync(name, ingestionId);        
            if (schedule !=null) obj.setSchedule(schedule); 
                       

            return obj;
        }		
    }
}
