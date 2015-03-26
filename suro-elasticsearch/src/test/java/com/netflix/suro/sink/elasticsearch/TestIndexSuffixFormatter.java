package com.netflix.suro.sink.elasticsearch;

import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestIndexSuffixFormatter {
    @Test
    public void shouldNullTypeReturnsEmptyString() {
        IndexSuffixFormatter formatter = new IndexSuffixFormatter(null, null);
        assertEquals(formatter.format(any(IndexInfo.class)), "");
    }

    @Test
    public void shouldDateTypeReturnsCorrectOne() {
        System.setProperty("user.timezone", "GMT");

        Properties props = new Properties();
        props.put("dateFormat", "YYYYMMdd");

        DateTime dt = new DateTime("2014-10-12T00:00:00.000Z");
        IndexSuffixFormatter formatter = new IndexSuffixFormatter("date", props);
        IndexInfo info = mock(IndexInfo.class);
        doReturn(dt.getMillis()).when(info).getTimestamp();

        assertEquals(formatter.format(info), "20141012");
    }

    @Test(expected=RuntimeException.class)
    public void shouldThrowExceptionOnUnsupportedType() {
        IndexSuffixFormatter formatter = new IndexSuffixFormatter("invalid", null);
    }

    @Test
    public void testWeeklyRepresentation() {
        System.setProperty("user.timezone", "GMT");

        Properties props = new Properties();
        props.put("dateFormat", "YYYYMM_ww");

        DateTime dt = new DateTime("2014-10-12T00:00:00.000Z");
        IndexSuffixFormatter formatter = new IndexSuffixFormatter("date", props);
        IndexInfo info = mock(IndexInfo.class);
        doReturn(dt.getMillis()).when(info).getTimestamp();

        assertEquals(formatter.format(info), "201410_41");
    }
}
