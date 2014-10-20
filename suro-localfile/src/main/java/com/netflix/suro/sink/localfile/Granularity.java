/*
 * Copyright 2013 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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
