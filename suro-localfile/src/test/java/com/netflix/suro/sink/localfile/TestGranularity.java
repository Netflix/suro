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

import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by liuzhenchuan@foxmail.com on 10/20/14.
 */
public class TestGranularity {

    @Test
    public void testNextHour(){
        DateTime dateTime = new DateTime("2014-10-18T15:48:55.000+08:00");
        DateTime expected = new DateTime("2014-10-18T16:00:00.000+08:00");
        assertEquals(Granularity.HOUR.next(dateTime),expected);

        dateTime = new DateTime("2014-10-18T23:48:55.000+08:00");
        expected = new DateTime("2014-10-19T00:00:00.000+08:00");
        assertEquals(Granularity.HOUR.next(dateTime),expected);
    }

    @Test
    public void testNextDay(){
        DateTime dateTime = new DateTime("2014-10-18T15:48:55.000+08:00");
        DateTime expected = new DateTime("2014-10-19T00:00:00.000+08:00");
        assertEquals(Granularity.DAY.next(dateTime),expected);

        dateTime = new DateTime("2014-10-18T00:00:00.000+08:00");
        expected = new DateTime("2014-10-19T00:00:00.000+08:00");
        assertEquals(Granularity.DAY.next(dateTime),expected);
    }

}