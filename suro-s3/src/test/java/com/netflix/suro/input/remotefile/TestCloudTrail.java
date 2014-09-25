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
                "  \"MessageId\" : \"c1ba2bdb-17da-5386-92c3-674d7f76a145\",\n" +
                "  \"TopicArn\" : \"arn:aws:sns:eu-west-1:267481021547:nflx-cloudtrail\",\n" +
                "  \"Message\" : \"{\\\"s3Bucket\\\":\\\"nflx-aws-eagle-awstest\\\",\\\"s3ObjectKey\\\":[\\\"awscontentplatform/AWSLogs/267481021547/CloudTrail/eu-west-1/2014/09/19/267481021547_CloudTrail_eu-west-1_20140919T1725Z_pz1McETnETNesdVz.json.gz\\\"]}\",\n" +
                "  \"Timestamp\" : \"2014-09-19T17:22:52.430Z\",\n" +
                "  \"SignatureVersion\" : \"1\",\n" +
                "  \"Signature\" : \"jB6HTe4lldWKtBuQYLWEX9cVyNV4eoSKSRIcdM/ZogYHXA9JTgQmXpf1gwhf4O9Lpo66lRSGsSsGLDkpWXN94mDknK/HWV63U+o+daTUVwnJ0PoA1ZalDfEz1ScpsmQ3Ttk3gRIT3uNN0wX1hSaUUeSwDPPfDLzf+LMZFpmGOckCqo8C6L3gRV4lx8pNgJN8Q0J6GtgkQqOA7ni5AT5g4NH9tObdHvoGb4+TQX4+enWM5khqjzlQKAZm9hH5sAUmABcTt9HrGWraUnxHohlkyDxnLomcWeiuFsMDzuw94LDyFB/CyzPYvtcPYBZR3tYuOD4jPH9fMnEr/mRunUSKmA==\",\n" +
                "  \"SigningCertURL\" : \"https://sns.eu-west-1.amazonaws.com/SimpleNotificationService-d6d679a1d18e95c2f9ffcf11f4f9e198.pem\",\n" +
                "  \"UnsubscribeURL\" : \"https://sns.eu-west-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-west-1:267481021547:nflx-cloudtrail:f5becd92-67d3-43f6-8eb9-c4d1c011fb93\"\n" +
                "}";

        S3Consumer s3Consumer = new S3Consumer(null, null, new QueueNotice(0, 0), 0, 0, null, new RecordParser() {
            @Override
            public List<MessageContainer> parse(String data) {
                return null;
            }
        }, null, null, new DefaultObjectMapper(), null);
        Map<String, Object> inMsg = s3Consumer.parseMessage(new Pair<String, String>("", msg));

        assertEquals(inMsg.get("s3Bucket"), "nflx-aws-eagle-awstest");
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
