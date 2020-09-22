package io.epoxi.repository;

import com.google.cloud.scheduler.v1.Job;
import com.google.cloud.scheduler.v1.LocationName;
import io.epoxi.cloud.logging.AppLog;
import io.epoxi.cloud.logging.StatusCode;
import io.epoxi.cloud.pubsub.PubsubTopic;
import io.epoxi.cloud.pubsub.PubsubWriter;
import io.epoxi.cloud.scheduler.JobScheduler;
import io.epoxi.repository.exception.RepositoryException;
import io.epoxi.repository.model.Ingestion;
import io.epoxi.repository.model.IngestionSync;
import io.epoxi.repository.model.IngestionSyncProgress;
import io.epoxi.repository.modelbase.EngineRepository;
import io.epoxi.repository.modelbase.MemberRepository;

import java.lang.invoke.MethodHandles;
import java.util.Optional;

public class IngestionSyncRepository extends MemberRepository<IngestionSync> {

    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

    public IngestionSyncRepository(EngineRepository<?> parentRepository)
	  {
      super(IngestionSync.class, parentRepository);
    }

    /**
     * Either start or schedule in an ingestion.  
     * If there is no schedule in the sync object, then an ingestion message will be added to the source queue.
     * If there is a schedule, an item will be created in the scheduler.  At the scheduled moment, the scheduled action
     * re-call the add method (with no schedule) so that an ingestion may be queued at that moment
     * @param ingestionSync The sync to add
     */
    @Override
    public synchronized void add(IngestionSync ingestionSync) {
      
      //Schedule the ingestion, or else queue the ingestion
      Optional.ofNullable(ingestionSync.getSchedule()).ifPresentOrElse(s -> scheduleIngestion(ingestionSync), () -> add(ingestionSync.getIngestion())); 
    }

    /**
     * Queue the ingestion for immediate processing
     * @param ingestion The ingestion to add
     */
    public void add(Ingestion ingestion)
    {
      try
      {
        PubsubTopic topic = PubsubTopic.of(IngestionSync.getIngestionTopic());
        PubsubWriter writer = topic.getWriter();
        writer.write(ingestion);
      }
      catch (InterruptedException ex)
      {
        logger.atError().log("Queuing of Ingestion was interrupted.");
        Thread.currentThread().interrupt();
        throw new RepositoryException("Queuing of Ingestion was interrupted", StatusCode.CANCELLED);
      }
    }

    @Override
    public void delete(IngestionSync ingestionSync, Boolean permanent) {

      JobScheduler jobScheduler = Optional.ofNullable(ingestionSync.getSchedule()).orElseThrow().getJobScheduler();

      try
      {        
        //Delete the GCP job
        jobScheduler.delete();

        //Update the metadata
        super.delete(ingestionSync, permanent);
      }
      catch(Exception ex)
      {
        ingestionSync.setState(IngestionSync.SyncState.FAILED);
        super.add(ingestionSync);     
      }
    }

    public void pauseIngestionSchedule(Long id) {

      IngestionSync ingestionSync = get(id);
      setIngestionState(ingestionSync, IngestionSync.SyncState.PAUSED);
    }

    public void resumeIngestionSchedule(Long id) {

      IngestionSync ingestionSync = get(id);
      setIngestionState(ingestionSync, IngestionSync.SyncState.ENABLED);
    }

    private void setIngestionState(IngestionSync ingestionSync, IngestionSync.SyncState state)
    {
      ingestionSync.setState(state);

      //Save the new state
      scheduleIngestion(ingestionSync);
    }

    /*
     * Stops an in-process ingestion.  If no ingestion is in process, no action is taken and no error is thrown
     * @param id The id of the Ingestion Schedule
     * @param now  If false, the current work in process is completed, and no new work is performed. If true, The thread is immediately terminated, without orderly shutdown
     */
    public void shutdownIngestionSchedule(Long id, Boolean now) {
      throw new UnsupportedOperationException("Interrupts are not yet implemented by the API");
    }
    
    //If there is a sync in progress, report is progress
    public IngestionSyncProgress progress(Long id) {
      throw new UnsupportedOperationException("Get progress are not yet implemented by the API");			
    }	
    

    /**
     * Schedule the ingestion to run at a specific time
     * @param ingestionSync The ingestionSync that specifies the schedule for an ingestion
     */
    private void scheduleIngestion(IngestionSync ingestionSync)
    {
      //Save the ingestionSync to the DataStore
      super.add(ingestionSync);

      //Create (or update) the job in CloudScheduler
      try
      {        
        JobScheduler jobScheduler = Optional.ofNullable(ingestionSync.getSchedule()).orElseThrow().getJobScheduler();
        if (Boolean.FALSE.equals(jobScheduler.jobExists()))
        {
          Job job = ingestionSync.toScheduledJob();
          JobScheduler.createJob(getLocationName(), job);          
        }
        else
        {
          IngestionSync.SyncState state = ingestionSync.getState();

          switch(state)
          {
            case UNSPECIFIED :        
            jobScheduler.delete();
              break;
            case FAILED : 
            case PAUSED :
            jobScheduler.pause();
              break;
            case ENABLED :
            jobScheduler.resume();
              break;
          }
        }
      }
      catch(Exception ex)
      {
        ingestionSync.setState(IngestionSync.SyncState.FAILED);
        super.add(ingestionSync);
      }      
    }  

    private LocationName getLocationName()
    {
      return LocationName.of(Config.CLOUD_SCHEDULER_PROJECT_ID, Config.CLOUD_SCHEDULER_LOCATION_ID);      
    }

    
}