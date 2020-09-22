package io.epoxi.app.cloud.scheduler;

import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.cloud.scheduler.v1.CloudSchedulerClient;
import com.google.cloud.scheduler.v1.Job;
import com.google.cloud.scheduler.v1.JobName;
import com.google.cloud.scheduler.v1.LocationName;
import io.epoxi.app.cloud.logging.StatusCode;
import lombok.Getter;

import java.io.IOException;

public class JobScheduler {

    @Getter
    final JobName jobName;

    public JobScheduler(JobName jobName)
    {
        this.jobName = jobName;
    }

     /***
     * Creates a job in Google Cloud Scheduler based on the specified in the IngestionSync.
     * If the job already exists, it will be overwritten
     */
    public static void createJob(LocationName parent, Job job)
    {
      try (CloudSchedulerClient client = CloudSchedulerClient.create())
      {
          client.createJob(parent, job);
      }
      catch(IOException ex)
      {
        throw new SchedulerException("Cannot add scheduled job. Error accessing Cloud Scheduler", StatusCode.UNAVAILABLE);
      }
    }

    /**
     * Deletes the  Google Cloud Scheduler job.  If the job does not exist, no action will occur and no error will be thrown
     */
    public void delete()
    {
      if (Boolean.FALSE.equals(jobExists())) return;

      try (CloudSchedulerClient client = CloudSchedulerClient.create())
      {
        client.deleteJob(jobName);
      }
      catch(IOException ex)
      {
        throw new SchedulerException("Cannot remove scheduled job. Error accessing Cloud Scheduler", StatusCode.UNAVAILABLE);
      }
    }

    public void pause()
    {
      try (CloudSchedulerClient client = CloudSchedulerClient.create())
      {
        Job job = client.getJob(jobName);
        if (job==null)  throw new IllegalArgumentException(String.format("Job %s does not exist", jobName));

        client.pauseJob(jobName);
      }
      catch(IOException ex)
      {
        throw new SchedulerException("Cannot pause scheduled job. Error accessing Cloud Scheduler", StatusCode.UNAVAILABLE);
      }
    }

    public void resume()
    {
      try (CloudSchedulerClient client = CloudSchedulerClient.create())
      {
        Job job = client.getJob(jobName);
        if (job==null)  throw new IllegalArgumentException(String.format("Job %s does not exist", jobName));

        client.resumeJob(jobName);
      }
      catch(IOException ex)
      {
        throw new SchedulerException("Cannot resume scheduled job. Error accessing Cloud Scheduler", StatusCode.UNAVAILABLE);
      }
    }

     /*
     * Tests the existence of the specified Google Cloud Scheduler job.
     */
    public Job get()
    {
      try (CloudSchedulerClient client = CloudSchedulerClient.create())
      {
        return (client.getJob(jobName));
      }
      catch(IOException ex)
      {
        throw new SchedulerException("Cannot get scheduled job. Error accessing Cloud Scheduler", StatusCode.UNAVAILABLE);
      }
    }

     /*
     * Tests the existence of the specified Google Cloud Scheduler job.
     */
    public Job tryGet()
    {
      try (CloudSchedulerClient client = CloudSchedulerClient.create())
      {
        return (client.getJob(jobName));
      }
      catch(PermissionDeniedException | NotFoundException ex)
      {
        //eat the error
        return null;
      }
      catch(IOException ex)
      {
        throw new SchedulerException("Cannot get scheduled job. Error accessing Cloud Scheduler", StatusCode.UNAVAILABLE);
      }
    }

     /*
     * Tests the existence of the specified Google Cloud Scheduler job.
     */
    public Boolean jobExists()
    {
      try (CloudSchedulerClient client = CloudSchedulerClient.create())
      {
        return (client.getJob(jobName) != null);
      }
      catch(IOException ex)
      {
        throw new SchedulerException("Cannot determine the existence of the scheduled job. Error accessing Cloud Scheduler", StatusCode.UNAVAILABLE);
      }
      catch(PermissionDeniedException | NotFoundException ex)
      {
        //eat the error
        return false;
      }
    }
}
