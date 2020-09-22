package io.epoxi.app.cloud.scheduler;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.cloud.scheduler.v1.JobName;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

public class Schedule {

    public Schedule()
    {
    }

    public Schedule name(String name) {
        this.name = name;
        return this;
    }

    public Schedule detail(String detail) {
        this.detail = detail;
        return this;
    }

    public Schedule timezone(String timezone) {
        this.timezone = timezone;
        return this;
    }

    @JsonIgnore
    public JobName getJobName()
    {
        return JobName.of(projectId, location, getName().replace(" ", "_"));
    }

    @JsonIgnore
    public JobScheduler getJobScheduler()
    {
        JobName jobName = getJobName();
        return new JobScheduler(jobName);
    }

    @Override
    public String toString() {

        return  "class Schedule {\n" +
            "    name: " + toIndentedString(name) + "\n" +
            "    timezone: " + toIndentedString(timezone) + "\n" +
            "    detail: " + toIndentedString(detail) + "\n" +
            "}";
    }

    /*
     * Convert the given object to string with each line indented by 4 spaces
     * (except the first line).
     */
    private static String toIndentedString(java.lang.Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }

    public static Builder newBuilder(String projectId, String location)
    {
        return new Builder(projectId, location);
    }

    private String projectId;
    private String location;

    @Getter @Setter
    private String name = null;

    @Getter @Setter
    private String detail = null;  //"0 9 * * 1"; // Every Monday at 9am, UTC.

    @Getter @Setter
    private String timezone = null;  // "UTC"


    /***********************************************
     * Schedule Builder
     **********************************************/
    public static class Builder
    {
        String projectId;
        String location;

        String name;
        String timezone = "America/Chicago";

        String detail = "*";
        String second = "*";
        String minute = "*";
        String hour = "*";
        String dayOfMonth = "*";
        String month = "*";
        String dayOfWeek = "*";
        String year = "";

        private Builder(String projectId, String location)
        {
            this.projectId = projectId;
            this.location = location;

            patterns.put("minute", "[0-5]?\\d");
            patterns.put("hour", "[01]?\\d|2[0-3]");
            patterns.put("dayOfMonth", "0?[1-9]|[12]\\d|3[01]");
            patterns.put("month", "[1-9]|1[012]");
            patterns.put("dayOfWeek", "[0-7]");
        }

        /****
         * Sets the timezone for the schedule
         * @param timezone A valid timezone entry from https://en.wikipedia.org/wiki/Tz_database#Names_of_time_zones e.g. America/Chicago
         * @return A fluid builder for the Schedule
         */
        public Builder setTimezone (String timezone)
        {
            // String pattern = "^(?:Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])$";  TODO : Validate Timezones that appear in the format "America/Chicago";
            // if (Boolean.FALSE.equals(timezone.matches(pattern)))
            // {
            //     throw new IllegalArgumentException(String.format("Cannot set timezone value. '%s' is not a valid value.", timezone));
            // }

            this.timezone = timezone;
            return this;
        }

        /**
         *  Pass a string that can be split into 6 (optionally 7) parts.  Each part must be in a specified format
         *
         *  Special Characters
         *  * (all) – it is used to specify that event should happen for every time unit. For example, “*” in the <minute> field – means “for every minute”
         *  ? (any) – it is utilized in the <day-of-month> and <day-of -week> fields to denote the arbitrary value – neglect the field value. For example, if we want to fire a script at “5th of every month” irrespective of what the day of the week falls on that date, then we specify a “?” in the <day-of-week> field
         *  – (range) – it is used to determine the value range. For example, “10-11” in <hour> field means “10th and 11th hours”
         *  , (values) – it is used to specify multiple values. For example, “MON, WED, FRI” in <day-of-week> field means on the days “Monday, Wednesday, and Friday”
         *  / (increments) – it is used to specify the incremental values. For example, a “5/15” in the <minute> field, means at “5, 20, 35 and 50 minutes of an hour”
         *  L (last) – it has different meanings when used in various fields. For example, if it's applied in the <day-of-month> field, then it means last day of the month, i.e. “31st for January” and so on as per the calendar month. It can be used with an offset value, like “L-3“, which denotes the “third to last day of the calendar month”. In the <day-of-week>, it specifies the “last day of a week”. It can also be used with another value in <day-of-week>, like “6L“, which denotes the “last Friday”
         *  W (weekday) – it is used to specify the weekday (Monday to Friday) nearest to a given day of the month. For example, if we specify “10W” in the <day-of-month> field, then it means the “weekday near to 10th of that month”. So if “10th” is a Saturday, then the job will be triggered on “9th”, and if “10th” is a Sunday, then it will trigger on “11th”. If you specify “1W” in the <day-of-month> and if “1st” is Saturday, then the job will be triggered on “3rd” which is Monday, and it will not jump back to the previous month
         *  # – it is used to specify the “N-th” occurrence of a weekday of the month, for example, “3rd Friday of the month” can be indicated as “6#3“
         * @param detail A formatted string matching a cron schedule
         * @return A fluid builder for the Schedule
         */
        public Builder setDetail (String detail)
        {
            String[] parts = detail.split(" ");
            if (Boolean.TRUE.equals(parts.length == 5))
            {
                setDetail(parts[0], parts[1], parts[2], parts[3], parts[4]);
            }
            else
            {
                throw new IllegalArgumentException("Invalid detail string");
            }

           return this;
        }

        public Builder setDetail (String minute, String hour, String dayOfMonth, String month, String dayOfWeek)
        {
            setDetailMinute(minute);
            setDetailHour(hour);
            setDetailDayOfMonth(dayOfMonth);
            setDetailMonth(month);
            setDetailDayOfWeek(dayOfWeek);

            return this;
        }

        public Builder setDetailMinute(String minute)
        {
            validate("minute", minute);
            this.minute = minute;
            return this;
        }

        public Builder setDetailHour(String hour)
        {
            validate("hour", hour);
            this.hour = hour;
            return this;
        }

        public Builder setDetailDayOfMonth(String dayOfMonth)
        {
            validate("dayOfMonth", dayOfMonth);
            this.dayOfMonth = dayOfMonth;
            return this;
        }

        public Builder setDetailMonth(String month)
        {
            validate("month", month);
            this.month = month;
            return this;
        }

        public Builder setDetailDayOfWeek(String dayOfWeek)
        {
            validate("dayOfWeek", dayOfWeek);
            this.dayOfWeek = dayOfWeek;
            return this;
        }

        private String createDetail()
        {
            return String.format("%s %s %s %s %s",
             minute,
             hour,
             dayOfMonth,
             month,
             dayOfWeek).trim();
        }

        public Schedule build(){

            Schedule obj = new Schedule();
            obj.projectId = projectId;
            obj.location = location;
            obj.setName(name);
            obj.setTimezone(timezone);
            obj.setDetail(createDetail());

            return obj;
        }

        public void validate(String partName, String stringToValidate) {

            String pattern = patterns.get(partName);
            if (Boolean.FALSE.equals(stringToValidate.equals("*")) && Boolean.FALSE.equals(stringToValidate.matches(pattern)))
                throw new IllegalArgumentException(String.format("Cannot set %s value. '%s' is not a valid value.", partName, stringToValidate));
        }

        private static final Map<String, String> patterns = new HashMap<>();

		public Builder setName(String objectName) {
            this.name = objectName;
			return this;
        }

    }


}
