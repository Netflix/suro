package com.netflix.suro.sink.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import org.joda.time.*;
import org.joda.time.field.DividedDateTimeField;
import org.joda.time.field.OffsetDateTimeField;
import org.joda.time.field.ScaledDurationField;
import org.joda.time.format.*;

import java.util.Locale;
import java.util.Map;

public class TimestampField {
    private final String fieldName;
    private final FormatDateTimeFormatter formatter;

    @JsonCreator
    public TimestampField(@JsonProperty("field") String fieldName, @JsonProperty("format") String format) {
        if (format == null) {
            format = "dateOptionalTime";
        }

        this.fieldName = fieldName;
        this.formatter = Joda.forPattern(format);
    }

    public long get(Map<String, Object> msg) {
        Object value = msg.get(fieldName);

        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return formatter.parser().parseMillis(value.toString());
    }

    public static class Joda {

        public static FormatDateTimeFormatter forPattern(String input) {
            return forPattern(input, Locale.ROOT);
        }

        /**
         * Parses a joda based pattern, including some named ones (similar to the built in Joda ISO ones).
         */
        public static FormatDateTimeFormatter forPattern(String input, Locale locale) {
            if (!Strings.isNullOrEmpty(input)) {
                input = input.trim();
            }
            if (input == null || input.length() == 0) {
                throw new IllegalArgumentException("No date pattern provided");
            }

            DateTimeFormatter formatter;
            if ("basicDate".equals(input) || "basic_date".equals(input)) {
                formatter = ISODateTimeFormat.basicDate();
            } else if ("basicDateTime".equals(input) || "basic_date_time".equals(input)) {
                formatter = ISODateTimeFormat.basicDateTime();
            } else if ("basicDateTimeNoMillis".equals(input) || "basic_date_time_no_millis".equals(input)) {
                formatter = ISODateTimeFormat.basicDateTimeNoMillis();
            } else if ("basicOrdinalDate".equals(input) || "basic_ordinal_date".equals(input)) {
                formatter = ISODateTimeFormat.basicOrdinalDate();
            } else if ("basicOrdinalDateTime".equals(input) || "basic_ordinal_date_time".equals(input)) {
                formatter = ISODateTimeFormat.basicOrdinalDateTime();
            } else if ("basicOrdinalDateTimeNoMillis".equals(input) || "basic_ordinal_date_time_no_millis".equals(input)) {
                formatter = ISODateTimeFormat.basicOrdinalDateTimeNoMillis();
            } else if ("basicTime".equals(input) || "basic_time".equals(input)) {
                formatter = ISODateTimeFormat.basicTime();
            } else if ("basicTimeNoMillis".equals(input) || "basic_time_no_millis".equals(input)) {
                formatter = ISODateTimeFormat.basicTimeNoMillis();
            } else if ("basicTTime".equals(input) || "basic_t_Time".equals(input)) {
                formatter = ISODateTimeFormat.basicTTime();
            } else if ("basicTTimeNoMillis".equals(input) || "basic_t_time_no_millis".equals(input)) {
                formatter = ISODateTimeFormat.basicTTimeNoMillis();
            } else if ("basicWeekDate".equals(input) || "basic_week_date".equals(input)) {
                formatter = ISODateTimeFormat.basicWeekDate();
            } else if ("basicWeekDateTime".equals(input) || "basic_week_date_time".equals(input)) {
                formatter = ISODateTimeFormat.basicWeekDateTime();
            } else if ("basicWeekDateTimeNoMillis".equals(input) || "basic_week_date_time_no_millis".equals(input)) {
                formatter = ISODateTimeFormat.basicWeekDateTimeNoMillis();
            } else if ("date".equals(input)) {
                formatter = ISODateTimeFormat.date();
            } else if ("dateHour".equals(input) || "date_hour".equals(input)) {
                formatter = ISODateTimeFormat.dateHour();
            } else if ("dateHourMinute".equals(input) || "date_hour_minute".equals(input)) {
                formatter = ISODateTimeFormat.dateHourMinute();
            } else if ("dateHourMinuteSecond".equals(input) || "date_hour_minute_second".equals(input)) {
                formatter = ISODateTimeFormat.dateHourMinuteSecond();
            } else if ("dateHourMinuteSecondFraction".equals(input) || "date_hour_minute_second_fraction".equals(input)) {
                formatter = ISODateTimeFormat.dateHourMinuteSecondFraction();
            } else if ("dateHourMinuteSecondMillis".equals(input) || "date_hour_minute_second_millis".equals(input)) {
                formatter = ISODateTimeFormat.dateHourMinuteSecondMillis();
            } else if ("dateOptionalTime".equals(input) || "date_optional_time".equals(input)) {
                // in this case, we have a separate parser and printer since the dataOptionalTimeParser can't print
                // this sucks we should use the root local by default and not be dependent on the node
                return new FormatDateTimeFormatter(input,
                    ISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC),
                    ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC), locale);
            } else if ("dateTime".equals(input) || "date_time".equals(input)) {
                formatter = ISODateTimeFormat.dateTime();
            } else if ("dateTimeNoMillis".equals(input) || "date_time_no_millis".equals(input)) {
                formatter = ISODateTimeFormat.dateTimeNoMillis();
            } else if ("hour".equals(input)) {
                formatter = ISODateTimeFormat.hour();
            } else if ("hourMinute".equals(input) || "hour_minute".equals(input)) {
                formatter = ISODateTimeFormat.hourMinute();
            } else if ("hourMinuteSecond".equals(input) || "hour_minute_second".equals(input)) {
                formatter = ISODateTimeFormat.hourMinuteSecond();
            } else if ("hourMinuteSecondFraction".equals(input) || "hour_minute_second_fraction".equals(input)) {
                formatter = ISODateTimeFormat.hourMinuteSecondFraction();
            } else if ("hourMinuteSecondMillis".equals(input) || "hour_minute_second_millis".equals(input)) {
                formatter = ISODateTimeFormat.hourMinuteSecondMillis();
            } else if ("ordinalDate".equals(input) || "ordinal_date".equals(input)) {
                formatter = ISODateTimeFormat.ordinalDate();
            } else if ("ordinalDateTime".equals(input) || "ordinal_date_time".equals(input)) {
                formatter = ISODateTimeFormat.ordinalDateTime();
            } else if ("ordinalDateTimeNoMillis".equals(input) || "ordinal_date_time_no_millis".equals(input)) {
                formatter = ISODateTimeFormat.ordinalDateTimeNoMillis();
            } else if ("time".equals(input)) {
                formatter = ISODateTimeFormat.time();
            } else if ("tTime".equals(input) || "t_time".equals(input)) {
                formatter = ISODateTimeFormat.tTime();
            } else if ("tTimeNoMillis".equals(input) || "t_time_no_millis".equals(input)) {
                formatter = ISODateTimeFormat.tTimeNoMillis();
            } else if ("weekDate".equals(input) || "week_date".equals(input)) {
                formatter = ISODateTimeFormat.weekDate();
            } else if ("weekDateTime".equals(input) || "week_date_time".equals(input)) {
                formatter = ISODateTimeFormat.weekDateTime();
            } else if ("weekyear".equals(input) || "week_year".equals(input)) {
                formatter = ISODateTimeFormat.weekyear();
            } else if ("weekyearWeek".equals(input)) {
                formatter = ISODateTimeFormat.weekyearWeek();
            } else if ("year".equals(input)) {
                formatter = ISODateTimeFormat.year();
            } else if ("yearMonth".equals(input) || "year_month".equals(input)) {
                formatter = ISODateTimeFormat.yearMonth();
            } else if ("yearMonthDay".equals(input) || "year_month_day".equals(input)) {
                formatter = ISODateTimeFormat.yearMonthDay();
            } else if (!Strings.isNullOrEmpty(input) && input.contains("||")) {
                String[] formats = input.split("\\|\\|");
                DateTimeParser[] parsers = new DateTimeParser[formats.length];

                if (formats.length == 1) {
                    formatter = forPattern(input, locale).parser();
                } else {
                    DateTimeFormatter dateTimeFormatter = null;
                    for (int i = 0; i < formats.length; i++) {
                        FormatDateTimeFormatter currentFormatter = forPattern(formats[i], locale);
                        DateTimeFormatter currentParser = currentFormatter.parser();
                        if (dateTimeFormatter == null) {
                            dateTimeFormatter = currentFormatter.printer();
                        }
                        parsers[i] = currentParser.getParser();
                    }

                    DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder().append(dateTimeFormatter.withZone(DateTimeZone.UTC).getPrinter(), parsers);
                    formatter = builder.toFormatter();
                }
            } else {
                try {
                    formatter = DateTimeFormat.forPattern(input);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Invalid format: [" + input + "]: " + e.getMessage(), e);
                }
            }

            return new FormatDateTimeFormatter(input, formatter.withZone(DateTimeZone.UTC), locale);
        }


        public static final DurationFieldType Quarters = new DurationFieldType("quarters") {
            private static final long serialVersionUID = -8167713675442491871L;

            public DurationField getField(Chronology chronology) {
                return new ScaledDurationField(chronology.months(), Quarters, 3);
            }
        };

        public static final DateTimeFieldType QuarterOfYear = new DateTimeFieldType("quarterOfYear") {
            private static final long serialVersionUID = -5677872459807379123L;

            public DurationFieldType getDurationType() {
                return Quarters;
            }

            public DurationFieldType getRangeDurationType() {
                return DurationFieldType.years();
            }

            public DateTimeField getField(Chronology chronology) {
                return new OffsetDateTimeField(new DividedDateTimeField(new OffsetDateTimeField(chronology.monthOfYear(), -1), QuarterOfYear, 3), 1);
            }
        };
    }

    public static class FormatDateTimeFormatter {

        private final String format;

        private final DateTimeFormatter parser;

        private final DateTimeFormatter printer;

        private final Locale locale;

        public FormatDateTimeFormatter(String format, DateTimeFormatter parser, Locale locale) {
            this(format, parser, parser, locale);
        }

        public FormatDateTimeFormatter(String format, DateTimeFormatter parser, DateTimeFormatter printer, Locale locale) {
            this.format = format;
            this.locale = locale;
            this.printer = locale == null ? printer.withDefaultYear(1970) : printer.withLocale(locale).withDefaultYear(1970);
            this.parser = locale == null ? parser.withDefaultYear(1970) : parser.withLocale(locale).withDefaultYear(1970);
        }

        public String format() {
            return format;
        }

        public DateTimeFormatter parser() {
            return parser;
        }

        public DateTimeFormatter printer() {
            return this.printer;
        }

        public Locale locale() {
            return locale;
        }
    }


}
