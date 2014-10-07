package com.netflix.suro.sink.localfile;

import org.joda.time.*;

/**
 * Created by liuzhenchuan@foxmail.com on 9/30/14.
 */
public enum Granularity {

    HOUR{
        @Override
        public DateTime truncate(DateTime time) {
            final MutableDateTime mutableDateTime = time.toMutableDateTime();
            mutableDateTime.setMillisOfSecond(0);
            mutableDateTime.setSecondOfMinute(0);
            mutableDateTime.setMinuteOfHour(0);
            return mutableDateTime.toDateTime();
        }

        @Override
        public ReadablePeriod getUnits(int n) {
            return Hours.hours(n);
        }
    } ,
    DAY{
        @Override
        public DateTime truncate(DateTime time) {
            final MutableDateTime mutableDateTime = time.toMutableDateTime();
            mutableDateTime.setMillisOfDay(0);
            return mutableDateTime.toDateTime();
        }

        @Override
        public ReadablePeriod getUnits(int n) {
            return Days.days(n);
        }
    };

    public abstract DateTime truncate(DateTime time);
    public abstract ReadablePeriod getUnits(int n);

    public final DateTime next(DateTime time){
        return truncate(time.plus(getUnits(1)));
    }


}
