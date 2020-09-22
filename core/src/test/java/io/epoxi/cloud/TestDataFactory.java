package io.epoxi.cloud;

import com.google.cloud.scheduler.v1.Job;
import com.google.cloud.scheduler.v1.JobName;
import com.google.cloud.scheduler.v1.LocationName;
import com.google.cloud.scheduler.v1.PubsubTarget;
import com.google.cloud.scheduler.v1.Job.State;
import com.google.protobuf.ByteString;

import io.epoxi.cloud.scheduler.JobScheduler;
import io.epoxi.cloud.scheduler.Schedule;

public class TestDataFactory {

    public Job getTestScheduledJob(String name, Boolean saveIfNotExists)
    {
        JobName jobName = JobName.of(TestConfig.CLOUD_SCHEDULER_PROJECT_ID, TestConfig.CLOUD_SCHEDULER_LOCATION_ID, name);

        JobScheduler jobScheduler = new JobScheduler(jobName);

        Job job = jobScheduler.tryGet();
        if (job!=null) return job;

        return createTestScheduledJob(name, saveIfNotExists);
    }

    public Job createTestScheduledJob(String name)
    {
        return createTestScheduledJob(name, true);
    }

    public Job createTestScheduledJob(String name, Boolean save)
    {
      Schedule schedule = createTestSchedule(String.format("Schedule for %s", name));
      State jobState = State.DISABLED;

      ByteString data = ByteString.copyFromUtf8("Test message");

      // Other options are to create an App Engine HTTP target or a PubSub target.
      PubsubTarget pubsubTarget = PubsubTarget.newBuilder()
          .setTopicName(TestConfig.TEST_TOPIC)
          .setData(data)
          .build();

      Job job =  Job.newBuilder()
          .setName(schedule.getJobName().toString())
          .setDescription(String.format("Test Schedule for '%s'", name))
          .setSchedule(schedule.getDetail())
          .setTimeZone(schedule.getTimezone())
          .setState(jobState)
          .setPubsubTarget(pubsubTarget)
          .build();

        if (save)
        {
            //If the job exists, delete it
            JobScheduler jobScheduler = schedule.getJobScheduler();
            jobScheduler.delete();

            JobScheduler.createJob(LocationName.of(TestConfig.CLOUD_SCHEDULER_PROJECT_ID, TestConfig.CLOUD_SCHEDULER_LOCATION_ID), job);
        }

        return job;
    }

    public Schedule createTestSchedule(String name)
    {
        return  Schedule.newBuilder(TestConfig.CLOUD_SCHEDULER_PROJECT_ID, TestConfig.CLOUD_SCHEDULER_LOCATION_ID)
            .setName(name)
            .setTimezone("UTC")
            .setDetailHour("10")
            .build();
    }
}
