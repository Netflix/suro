package com.netflix.suro.sink.elasticsearch;

import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestTimestampField {
    @Test
    public void shouldNullFormatReturnsLongTS() {
        TimestampField field = new TimestampField("ts", null);
        long ts = System.currentTimeMillis();
        assertEquals(
                field.get(
                        new ImmutableMap.Builder<String, Object>()
                                .put("ts", ts)
                                .put("field1", "value1").build()),
                ts);
        assertEquals(
                field.get(
                        new ImmutableMap.Builder<String, Object>()
                                .put("ts", "2014-04-05T00:00:00.000Z")
                                .put("field1", "value1").build()),
                new DateTime("2014-04-05T00:00:00.000Z").getMillis());
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldNonNullFormatThrowsException() {
        TimestampField field = new TimestampField("ts", "YYYY-MM-DD");
        long ts = System.currentTimeMillis();
        assertEquals(
                field.get(
                        new ImmutableMap.Builder<String, Object>()
                                .put("ts", ts)
                                .put("field1", "value1").build()),
                ts);
        field.get(
                new ImmutableMap.Builder<String, Object>()
                        .put("ts", "2014-04-05T00:00:00.000Z")
                        .put("field1", "value1").build());
    }
}
