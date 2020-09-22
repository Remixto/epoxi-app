package io.epoxi.app.cloud.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.scheduler.v1.Job;
import com.google.cloud.scheduler.v1.JobName;
import com.google.cloud.scheduler.v1.LocationName;
import com.google.cloud.scheduler.v1.Job.State;

import io.epoxi.app.cloud.TestConfig;
import io.epoxi.app.cloud.TestDataFactory;
import org.junit.jupiter.api.Test;

public class JobSchedulerTest
{
    @Test
    public void createScheduleTest()
    {
        TestDataFactory factory = new TestDataFactory();
        Job job = factory.createTestScheduledJob("Test Job", false);

        JobScheduler jobScheduler = new JobScheduler(JobName.parse(job.getName()));
        jobScheduler.delete();

        LocationName parent = LocationName.of(TestConfig.CLOUD_SCHEDULER_PROJECT_ID, TestConfig.CLOUD_SCHEDULER_LOCATION_ID);
        JobScheduler.createJob(parent, job);
        assertTrue(jobScheduler.jobExists(),  "A job was scheduled successfully");
    }

    @Test
    public void deleteScheduleTest()
    {
        TestDataFactory factory = new TestDataFactory();
        Job job = factory.getTestScheduledJob("Test Job", true);

        JobScheduler jobScheduler = new JobScheduler(JobName.parse(job.getName()));
        jobScheduler.delete();

        assertFalse(jobScheduler.jobExists(),  "A job was deleted successfully");
    }

    @Test
    public void pauseScheduleTest()
    {
        TestDataFactory factory = new TestDataFactory();
        Job job = factory.getTestScheduledJob("Test Job", true);

        JobScheduler jobScheduler = new JobScheduler(JobName.parse(job.getName()));
        jobScheduler.pause();

        assertEquals(State.PAUSED,  jobScheduler.get().getState(),  "A job was paused successfully");
    }

    @Test
    public void resumeScheduleTest()
    {
        TestDataFactory factory = new TestDataFactory();
        Job job = factory.getTestScheduledJob("Test Job", true);

        JobScheduler jobScheduler = new JobScheduler(JobName.parse(job.getName()));
        jobScheduler.resume();

        assertEquals(State.ENABLED, jobScheduler.get().getState(),  "A job was resumed successfully");
    }

}
