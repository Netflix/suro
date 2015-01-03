package com.netflix.suro.sink.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.DefaultMessageContainer;
import com.netflix.suro.message.Message;
import com.netflix.suro.sink.Sink;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.client.http.JestHttpClient;
import io.searchbox.core.Count;
import io.searchbox.core.CountResult;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 1)
public class TestElasticSearchSink extends ElasticsearchIntegrationTest {
    protected JestClientFactory factory;
    protected JestHttpClient client;

    protected String getPort() {
        return "9200";
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1)
            .put(super.nodeSettings(nodeOrdinal)).build();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        factory = new JestClientFactory();
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://localhost:" + getPort()).multiThreaded(true).build();

        factory.setHttpClientConfig(httpClientConfig);

        client = (JestHttpClient) factory.getObject();
    }

    @Test
    public void testDefaultArgument() throws IOException {
        String index = "topic";

        createDefaultESSink(index);

        refresh();
        Count count = new Count.Builder()
            .addIndex(index)
            .build();
        CountResult result = client.execute(count);
        assertEquals(result.getCount().doubleValue(), 100.0, 0.0001);
    }

    private ElasticSearchSink createDefaultESSink(String index) throws JsonProcessingException {
        ObjectMapper jsonMapper = new DefaultObjectMapper();
        ElasticSearchSink sink = new ElasticSearchSink(
            null,
            10,
            1000,
            null,
            Lists.newArrayList("http://localhost:" + getPort()),
            null,
            0,0,0,0,
            null,
            jsonMapper
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
            Lists.newArrayList("http://localhost:" + getPort()),
            new DefaultIndexInfoBuilder(
                null,
                null,
                new TimestampField("ts", null),
                new IndexSuffixFormatter("date", props),
                null,
                jsonMapper),
            0,0,0,0,
            null,
            jsonMapper
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

        refresh();
        Count count = new Count.Builder()
            .addIndex(index)
            .build();
        CountResult result = client.execute(count);
        assertEquals(result.getCount().doubleValue(), 100.0, 0.0001);
    }

    @Test
    public void testCreate() throws IOException {
        String desc = "    {\n" +
            "        \"type\": \"elasticsearch\",\n" +
            "        \"queue4Sink\":{\"type\": \"memory\", \"capacity\": 10000 },\n" +
            "        \"batchSize\": 100,\n" +
            "        \"batchTimeout\": 1000,\n" +
            "        \"cluster.name\": \"es_test\",\n" +
            "        \"addressList\": [\"http://host1:port1\", \"http://host2:port2\"],\n" +
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
    public void testRecover() throws Exception {
        ObjectMapper jsonMapper = new DefaultObjectMapper();
        ElasticSearchSink sink = new ElasticSearchSink(
            null,
            10,
            1000,
            null,
            Lists.newArrayList("http://localhost:" + getPort()),
            null,
            0,0,0,0,
            null,
            jsonMapper
        );
        sink.open();

        DateTime dt = new DateTime("2014-10-12T12:12:12.000Z");

        Map<String, Object> msg = new ImmutableMap.Builder<String, Object>()
            .put("f1", "v1")
            .put("f2", "v2")
            .put("f3", "v3")
            .put("ts", dt.getMillis())
            .build();
        String routingKey = "topicrecover";
        String index = "topicrecover";
        List<Message> msgList = new ArrayList<>();
        int msgCount = 100;
        for (int i = 0; i < msgCount; ++i) {
            msgList.add(new Message(routingKey, jsonMapper.writeValueAsBytes(msg)));
        }

        for (Message m : msgList) {
            sink.recover(m);
        }

        refresh();
        Count count = new Count.Builder()
            .addIndex(index)
            .build();
        CountResult result = client.execute(count);
        assertEquals(result.getCount().doubleValue(), 100.0, 0.0001);
    }

    private ObjectMapper jsonMapper = new DefaultObjectMapper();

    @Test
    public void testStat() throws JsonProcessingException, InterruptedException {
        final long ts = System.currentTimeMillis() - 1;

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
                        public Object getSource() {
                            if (m.getRoutingKey().startsWith("rejected")) {
                                return m.getPayload();
                            } else {
                                return new String(m.getPayload());
                            }
                        }

                        @Override
                        public String getId() {
                            return null;
                        }

                        @Override
                        public long getTimestamp() {
                            return ts;
                        }
                    };
                }
            }
        }).when(indexInfo).create(any(Message.class));

        ElasticSearchSink sink = new ElasticSearchSink(
            null, // by default it will be memory queue
            1000,
            5000,
            "cluster",
            Lists.newArrayList("http://localhost:" + getPort()),
            indexInfo,
            0,0,0,0,
            null,
            jsonMapper);
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
        System.out.println(stat);
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

        // check indexDelay section
        ArrayIterator iterator = new ArrayIterator(stat.split("\n"));
        while (iterator.hasNext() && !iterator.next().equals("indexDelay"));
        Set<String> stringSet = new HashSet<>();
        for (int i = 0; i < 6; ++i) {
            String s = (String) iterator.next();
            assertTrue(Long.parseLong(s.split(":")[1]) > 0);
            stringSet.add(s.split(":")[0]);
        }
        assertEquals(stringSet.size(), 6);
    }

    private byte[] getAnyMessage() throws JsonProcessingException {
        return jsonMapper.writeValueAsBytes(new ImmutableMap.Builder<String, Object>().put("f1", "v1").build());
    }
}
