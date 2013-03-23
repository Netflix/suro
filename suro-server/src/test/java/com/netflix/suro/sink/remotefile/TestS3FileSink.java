package com.netflix.suro.sink.remotefile;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.StringSerDe;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.localfile.TestTextFileWriter;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.multi.s3.S3ServiceEventListener;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.utils.MultipartUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

public class TestS3FileSink {
    public static final String s3FileSink = "{\n" +
            "    \"type\": \"S3FileSink\",\n" +
            "    \"name\": \"s3\",\n" +
            "    \"localFileSink\": {\n" +
            "        \"type\": \"LocalFileSink\",\n" +
            "        \"name\": \"local\",\n" +
            "        \"outputDir\": \"" + TestTextFileWriter.dir + "\",\n" +
            "        \"writer\": {\n" +
            "            \"type\": \"text\"\n" +
            "        },\n" +
            "        \"maxFileSize\": 10240,\n" +
            "        \"rotationPeriod\": \"PT1m\",\n" +
            "        \"notify\": {\n" +
            "            \"type\": \"queue\"\n" +
            "        }\n" +
            "    },\n" +
            "    \"bucket\": \"s3bucket\",\n" +
            "    \"maxPartSize\": 10000,\n" +
            "    \"concurrentUpload\":5,\n" +
            "    \"notify\": {\n" +
            "        \"type\": \"queue\"\n" +
            "    },\n" +
            "    \"prefixFormatter\": {" +
            "    \"type\": \"DateRegionStack\",\n" +
            "    \"date\": \"YYYYMMDD\"}\n" +
            "}";

    @Before
    @After
    public void clean() throws IOException {
        TestTextFileWriter.cleanUp();
    }

    @Test
    public void test() throws Exception {
        ObjectMapper mapper = new DefaultObjectMapper();
        final Map<String, Object> injectables = Maps.newHashMap();

        injectables.put("region", "eu-west-1");
        injectables.put("stack", "gps");

        injectables.put("credentials", new AWSCredentials("accessKey", "secretKey"));

        MultipartUtils mpUtils = mock(MultipartUtils.class);
        doNothing().when(mpUtils).uploadObjects(
                any(String.class),
                any(RestS3Service.class),
                any(List.class),
                any(S3ServiceEventListener.class));
        injectables.put("multipartUtils", mpUtils);

        mapper.setInjectableValues(new InjectableValues() {
            @Override
            public Object findInjectableValue(
                    Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance
            ) {
                return injectables.get(valueId);
            }
        });
        Sink sink = mapper.readValue(s3FileSink, new TypeReference<Sink>(){});
        sink.open();

        for (int i = 0; i < 100000; ++i) {
            sink.writeTo(
                    new Message("routingKey", "app", "hostname", "datatype", ("message0" + i).getBytes()),
                    new StringSerDe());
        }
        sink.close();

        // check every file uploaded, deleted, and notified
        File dir = new File(TestTextFileWriter.dir);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File file, String name) {
                if (file.isFile() && file.getName().startsWith(".") == false) {
                   return true;
                } else {
                    return false;
                }
            }
        });
        assertEquals(files.length, 0);
        int count = 0;
        while (sink.recvNotify() != null) {
            ++count;
        }
        assertTrue(count > 0);
    }
}
