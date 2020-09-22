package io.epoxi.cloud.scheduler;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.epoxi.cloud.TestConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ScheduleTest {

    @Test
    public void createScheduleWithFullSpecificiation()
    {
        Schedule.Builder builder = Schedule.newBuilder(projectId, location);

        Assertions.assertDoesNotThrow(() -> builder.setTimezone("UTC"), "Timezone successfully set");

        Assertions.assertDoesNotThrow(() -> builder.setDetailMinute("1"), "Minute successfully set");
        Assertions.assertDoesNotThrow(() -> builder.setDetailHour("01"), "Hour successfully set");
        Assertions.assertDoesNotThrow(() -> builder.setDetailDayOfMonth("5"), "DayOfMonth successfully set");
        Assertions.assertDoesNotThrow(() -> builder.setDetailMonth("5"), "Month successfully set");
        Assertions.assertDoesNotThrow(() -> builder.setDetailDayOfWeek("2"), "DayOfWeek successfully set");

        Assertions.assertDoesNotThrow(() -> builder.build(), "Fully specified schedule object successfully created");
    }

    @Test
    public void createScheduleWithDetailParts()
    {
        Schedule.Builder builder = Schedule.newBuilder(projectId, location);
        Assertions.assertDoesNotThrow(() -> builder.setDetail("*", "*", "12", "1", "*"), "Detail successfully set");
        Assertions.assertDoesNotThrow(() -> builder.build(), "Fully specified schedule object successfully created detail parts");
    }

    @Test
    public void createScheduleWithDetailString()
    {
        Schedule.Builder builder = Schedule.newBuilder(projectId, location);
        Assertions.assertDoesNotThrow(() -> builder.setDetail("* * 12 1 *"), "Detail successfully set");
        Assertions.assertDoesNotThrow(() -> builder.build(), "Fully specified schedule object successfully created from detail string");
    }

    @Test
    public void createScheduleWithPartialSpecificiation()
    {
        Schedule schedule = Schedule.newBuilder(projectId, location)
            .setTimezone("UTC")
            .setDetailHour("10")
            .build();

        assertNotNull(schedule, "Partially specified schedule object successfully created");
    }

    @Test
    public void createScheduleWithInvalidSpecificiation()
    {

        Schedule.Builder builder =  Schedule.newBuilder(projectId, location);

        //Assert invalid Hour
        assertThrows(   IllegalArgumentException.class,
                        () -> builder.setDetailHour("33")
                   );

        //Assert invalid Day of Week
        assertThrows(   IllegalArgumentException.class,
                        () -> builder.setDetailDayOfWeek("T")
                     );

         //Assert invalid Timezone
         assertThrows(   IllegalArgumentException.class,
         () -> builder.setDetailDayOfWeek("sdfds")
      );
    }

    private final String projectId = TestConfig.CLOUD_SCHEDULER_PROJECT_ID;
    private final String location = TestConfig.CLOUD_SCHEDULER_LOCATION_ID;
}
