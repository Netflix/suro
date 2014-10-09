package com.netflix.suro.sink.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableMap;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.DefaultMessageContainer;
import com.netflix.suro.message.Message;
import com.netflix.suro.sink.Sink;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class TestElasticSearchSink {
    private static Node node;
    private static Settings nodeSettings() {
        return ImmutableSettings.settingsBuilder().build();
    }

    @BeforeClass
    public static void setup() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("gateway.type", "none")
                .put("index.store.type", "memory")
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(nodeSettings())
                .build();
        node = NodeBuilder.nodeBuilder().local(true).settings(settings).node();
    }

    @AfterClass
    public static void cleardown() {
        node.close();
    }

    @Test
    public void testDefaultArgument() throws JsonProcessingException {
        String index = "topic";

        createDefaultESSink(index);

        node.client().admin().indices().prepareRefresh(index).execute().actionGet();
        CountResponse response = node.client().prepareCount(index).execute().actionGet();
        assertEquals(response.getCount(), 100);
    }

    private ElasticSearchSink createDefaultESSink(String index) throws JsonProcessingException {
        ObjectMapper jsonMapper = new DefaultObjectMapper();
        ElasticSearchSink sink = new ElasticSearchSink(
                null,
                10,
                1000,
                null,
                true,
                "1s",
                "1s",
                null,
                null,
                0,0,0,0,
                null,
                jsonMapper,
                node.client()
        );
        sink.open();

        DateTime dt = new DateTime("2014-10-12T12:12:12.000Z");

        Map<String, Object> msg = new ImmutableMap.Builder<String, Object>()
                .put("f1", "v1")
                .put("f2", "v2")
                .put("f3", "v3")
                .put("ts", dt.getMillis())
                .build();

        for (int i = 0; i < 100; ++i) {
            sink.writeTo(new DefaultMessageContainer(new Message(index, jsonMapper.writeValueAsBytes(msg)), jsonMapper));
        }
        sink.close();
        return sink;
    }

    @Test
    public void testIndexInfoBuilder() throws IOException {
        ObjectMapper jsonMapper = new DefaultObjectMapper();
        Properties props = new Properties();
        props.setProperty("dateFormat", "YYYYMMdd");
        ElasticSearchSink sink = new ElasticSearchSink(
                null,
                1,
                1000,
                null,
                true,
                "1s",
                "1s",
                null,
                new DefaultIndexInfoBuilder(
                        null,
                        null,
                        new TimestampField("ts", null),
                        new IndexSuffixFormatter("date", props),
                        null,
                        jsonMapper),
                0,0,0,0,
                null,
                jsonMapper,
                node.client()
        );
        sink.open();

        DateTime dt = new DateTime("2014-10-12T12:12:12.000Z");

        Map<String, Object> msg = new ImmutableMap.Builder<String, Object>()
                .put("f1", "v1")
                .put("f2", "v2")
                .put("f3", "v3")
                .put("ts", dt.getMillis())
                .build();

        String routingKey = "topic";
        String index = "topic20141012";
        for (int i = 0; i < 100; ++i) {
            sink.writeTo(new DefaultMessageContainer(new Message(routingKey, jsonMapper.writeValueAsBytes(msg)), jsonMapper));
        }
        sink.close();

        node.client().admin().indices().prepareRefresh(index).execute().actionGet();
        CountResponse response = node.client().prepareCount(index).execute().actionGet();
        assertEquals(response.getCount(), 100);
    }

    @Test
    public void testCreate() throws IOException {
        String desc = "    {\n" +
                "        \"type\": \"elasticsearch\",\n" +
                "        \"queue4Sink\":{\"type\": \"memory\", \"capacity\": 10000 },\n" +
                "        \"batchSize\": 100,\n" +
                "        \"batchTimeout\": 1000,\n" +
                "        \"cluster.name\": \"es_test\",\n" +
                "        \"client.transport.sniff\": true,\n" +
                "        \"client.transport.ping_timeout\": \"60s\",\n" +
                "        \"client.transport_nodes_sampler_interval\": \"60s\",\n" +
                "        \"addressList\": [\"host1:port1\", \"host2:port2\"],\n" +
                "        \"indexInfo\":{\n" +
                "            \"type\": \"default\",\n" +
                "            \"indexTypeMap\":{\"routingkey1\":\"index1:type1\", \"routingkey2\":\"index2:type2\"},\n" +
                "            \"idFields\":{\"index\":[\"f1\", \"f2\"]},\n" +
                "            \"timestamp\": {\"field\":\"ts\"},\n" +
                "            \"indexSuffixFormatter\":{\"type\": \"date\", \"properties\":{\"dateFormat\":\"YYYYMMdd\"}}\n" +
                "        }\n" +
                "    }";
        final ObjectMapper jsonMapper = new DefaultObjectMapper();
        jsonMapper.registerSubtypes(new NamedType(ElasticSearchSink.class, "elasticsearch"));
        jsonMapper.setInjectableValues(new InjectableValues() {
            @Override
            public Object findInjectableValue(
                    Object valueId,
                    DeserializationContext ctxt,
                    BeanProperty forProperty,
                    Object beanInstance
            ) {
                if (valueId.equals(ObjectMapper.class.getCanonicalName())) {
                    return jsonMapper;
                } else {
                    return null;
                }
            }
        });

        Sink esSink = jsonMapper.readValue(desc, new TypeReference<Sink>(){});
        assertTrue(esSink instanceof ElasticSearchSink);
    }

    @Test
    public void testRecover() throws JsonProcessingException {
        ObjectMapper jsonMapper = new DefaultObjectMapper();
        ElasticSearchSink sink = new ElasticSearchSink(
                null,
                10,
                1000,
                null,
                true,
                "1s",
                "1s",
                null,
                null,
                0,0,0,0,
                null,
                jsonMapper,
                node.client()
        );

        DateTime dt = new DateTime("2014-10-12T12:12:12.000Z");

        Map<String, Object> msg = new ImmutableMap.Builder<String, Object>()
                .put("f1", "v1")
                .put("f2", "v2")
                .put("f3", "v3")
                .put("ts", dt.getMillis())
                .build();
        String routingKey = "topicrecover";
        String index = "topicrecover";
        List<Message> msgList = new ArrayList<Message>();
        int msgCount = 100;
        for (int i = 0; i < msgCount; ++i) {
            msgList.add(new Message(routingKey, jsonMapper.writeValueAsBytes(msg)));
        }

        BulkRequest request = sink.createBulkRequest(msgList);
        for (int i = 0; i < msgCount; ++i) {
            sink.recover(i, request);
        }

        node.client().admin().indices().prepareRefresh(index).execute().actionGet();
        CountResponse response = node.client().prepareCount(index).execute().actionGet();
        assertEquals(response.getCount(), 100);
    }

    private ObjectMapper jsonMapper = new DefaultObjectMapper();

    @Test
    public void testStat() throws JsonProcessingException, InterruptedException {
        IndexInfoBuilder indexInfo = mock(IndexInfoBuilder.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final Message m = (Message) invocation.getArguments()[0];
                if (m.getRoutingKey().startsWith("parsing_failed")) {
                    return null;
                } else {
                    return new IndexInfo() {
                        @Override
                        public String getIndex() {
                            return m.getRoutingKey();
                        }

                        @Override
                        public String getType() {
                            return "type";
                        }

                        @Override
                        public byte[] getSource() {
                            return m.getPayload();
                        }

                        @Override
                        public String getId() {
                            return null;
                        }

                        @Override
                        public long getTimestamp() {
                            return System.currentTimeMillis();
                        }
                    };
                }
            }
        }).when(indexInfo).create(any(Message.class));

        Client client = mock(Client.class);
        ActionFuture<BulkResponse> responseActionFuture = mock(ActionFuture.class);
        BulkResponse response = getBulkItemResponses();
        doReturn(response).when(responseActionFuture).actionGet();
        doReturn(responseActionFuture).when(client).bulk(any(BulkRequest.class));

        ActionFuture<IndexResponse> indexResponseActionFuture = mock(ActionFuture.class);
        doReturn(mock(IndexResponse.class)).when(indexResponseActionFuture).actionGet();
        doReturn(indexResponseActionFuture).when(client).index(any(IndexRequest.class));

        ElasticSearchSink sink = new ElasticSearchSink(
                null, // by default it will be memory queue
                1000,
                5000,
                "cluster",
                false,
                null,
                null,
                null,
                indexInfo,
                0,
                0,
                0,
                0,
                null,
                jsonMapper,
                client);
        sink.open();

        for (int i = 0; i < 3; ++i) {
            for (int j = 0; j < 3; ++j) {
                sink.writeTo(new DefaultMessageContainer(new Message("parsing_failed_topic" + i, getAnyMessage()), jsonMapper));
            }
            for (int j = 0; j < 3; ++j) {
                sink.writeTo(new DefaultMessageContainer(new Message("indexed" + i, getAnyMessage()), jsonMapper));
            }
            for (int j = 0; j < 3; ++j) {
                sink.writeTo(new DefaultMessageContainer(new Message("rejected" + i, getAnyMessage()), jsonMapper));
            }
        }

        sink.close();
        String stat = sink.getStat();
        int count = 0;
        for (int i = 0; i < 3; ++i) {
            for (int j = 0; j < 3; ++j) {
                if (stat.contains("parsing_failed_topic" + i + ":3")) {
                    ++count;
                }
            }
            for (int j = 0; j < 3; ++j) {
                if (stat.contains("indexed" + i + ":3")) {
                    ++count;
                }
            }
            for (int j = 0; j < 3; ++j) {
                if (stat.contains("rejected" + i + ":3")) {
                    ++count;
                }
            }
        }
        assertEquals(count, 27);
    }

    private BulkResponse getBulkItemResponses() {
        List<BulkItemResponse> responseList = new ArrayList<>();
        int id = 0;
        BulkItemResponse.Failure failure = mock(BulkItemResponse.Failure.class);
        doReturn("mocked failure message").when(failure).getMessage();
        for (int i = 0; i < 3; ++i) {
            for (int j = 0; j < 3; ++j) {
                responseList.add(new BulkItemResponse(
                        id++,
                        "insert",
                        mock(ActionResponse.class)
                ));
            }
            for (int j = 0; j < 3; ++j) {
                responseList.add(new BulkItemResponse(
                        id++,
                        "insert",
                        failure
                        ));
            }
        }
        return new BulkResponse(
                    responseList.toArray(new BulkItemResponse[responseList.size()]),
                    1000
            );
    }

    private byte[] getAnyMessage() throws JsonProcessingException {
        return jsonMapper.writeValueAsBytes(new ImmutableMap.Builder<String, Object>().put("f1", "v1").build());
    }
}
