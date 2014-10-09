package com.netflix.suro.input.remotefile;

import com.google.common.collect.ImmutableMap;
import com.netflix.suro.input.RecordParser;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.sink.notice.QueueNotice;
import com.netflix.util.Pair;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestCloudTrail {
    @Test
    public void testDefaultRoutingKey() throws Exception {
        CloudTrail cloudTrail = new CloudTrail(null, new DefaultObjectMapper());

        List<MessageContainer> messages = cloudTrail.parse("{\n" +
                "    \"Records\": [\n" +
                "        {\n" +
                "            \"msg1\": \"value1\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"msg2\": \"value2\"\n" +
                "        }\n" +
                "    ]\n" +
                "}");
        assertEquals(messages.size(), 2);
        assertEquals(messages.get(0).getRoutingKey(), "cloudtrail");
        assertEquals(messages.get(0).getEntity(S3Consumer.typeReference),
                new ImmutableMap.Builder<String, Object>().put("msg1", "value1").build());

        assertEquals(messages.get(1).getRoutingKey(), "cloudtrail");
        assertEquals(messages.get(1).getEntity(S3Consumer.typeReference),
                new ImmutableMap.Builder<String, Object>().put("msg2", "value2").build());


    }

    @Test
    public void testRoutingKey() throws Exception {
        CloudTrail cloudTrail = new CloudTrail("test", new DefaultObjectMapper());

        List<MessageContainer> messages = cloudTrail.parse("{\n" +
                "    \"Records\": [\n" +
                "        {\n" +
                "            \"msg1\": \"value1\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"msg2\": \"value2\"\n" +
                "        }\n" +
                "    ]\n" +
                "}");
        assertEquals(messages.size(), 2);
        assertEquals(messages.get(0).getRoutingKey(), "test");
        assertEquals(messages.get(0).getEntity(S3Consumer.typeReference),
                new ImmutableMap.Builder<String, Object>().put("msg1", "value1").build());

        assertEquals(messages.get(1).getRoutingKey(), "test");
        assertEquals(messages.get(1).getEntity(S3Consumer.typeReference),
                new ImmutableMap.Builder<String, Object>().put("msg2", "value2").build());
    }

    @Test
    public void testWithNonParseableMessage() {
        CloudTrail cloudTrail = new CloudTrail("test", new DefaultObjectMapper());

        // nothing should be thrown
        List<MessageContainer> messages = cloudTrail.parse("non_parseable_one");
        assertEquals(messages.size(), 0);
    }

    @Test
    public void testParsingMessage() throws IOException {
        String msg = "{\n" +
                "  \"Type\" : \"Notification\",\n" +
                "  \"MessageId\" : \"messageid\",\n" +
                "  \"TopicArn\" : \"topicarn\",\n" +
                "  \"Message\" : \"{\\\"s3Bucket\\\":\\\"bucket\\\",\\\"s3ObjectKey\\\":[\\\"objectkey\\\"]}\",\n" +
                "  \"Timestamp\" : \"2014-09-19T17:22:52.430Z\",\n" +
                "  \"SignatureVersion\" : \"1\",\n" +
                "  \"Signature\" : \"signature\",\n" +
                "  \"SigningCertURL\" : \"signingcerturl\",\n" +
                "  \"UnsubscribeURL\" : \"unsubscribeurl\"\n" +
                "}";

        S3Consumer s3Consumer = new S3Consumer(null, null, new QueueNotice(0, 0), 0, 0, null, new RecordParser() {
            @Override
            public List<MessageContainer> parse(String data) {
                return null;
            }
        }, null, null, new DefaultObjectMapper(), null);
        Map<String, Object> inMsg = s3Consumer.parseMessage(new Pair<String, String>("", msg));

        assertEquals(inMsg.get("s3Bucket"), "bucket");
    }

//    @Test
//    public void testWithActualBigFile() throws Exception {
//        String theString = IOUtils.toString(
//                new GZIPInputStream(
//                        new FileInputStream(
//                                new File("179727101194_CloudTrail_us-west-2_20140911T2145Z_jkY7QUM0hAZ89sf7.json.gz"))));
//
//        CloudTrail cloudTrail = new CloudTrail("testWithActualBigFile", new DefaultObjectMapper());
//        List<MessageContainer> messages = cloudTrail.parse(theString);
//        assertEquals(messages.size(), StringUtils.countMatches(theString, "eventTime"));
//        System.out.println(messages.size());
//        for (MessageContainer msg : messages) {
//            assertEquals(msg.getRoutingKey(), "testWithActualBigFile");
//            assertNotNull(msg.getEntity(S3Consumer.typeReference).get("eventTime"));
//        }
//    }
}
