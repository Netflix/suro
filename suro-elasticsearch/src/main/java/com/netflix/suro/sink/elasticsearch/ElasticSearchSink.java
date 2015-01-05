package com.netflix.suro.sink.elasticsearch;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.netflix.client.ClientFactory;
import com.netflix.client.http.HttpRequest;
import com.netflix.client.http.HttpResponse;
import com.netflix.config.ConfigurationManager;
import com.netflix.niws.client.http.RestClient;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.*;
import com.netflix.suro.TagKey;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.queue.MemoryQueue4Sink;
import com.netflix.suro.queue.MessageQueue4Sink;
import com.netflix.suro.servo.Servo;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.ThreadPoolQueuedSink;
import com.netflix.util.Pair;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ElasticSearchSink extends ThreadPoolQueuedSink implements Sink {
    private static Logger log = LoggerFactory.getLogger(ElasticSearchSink.class);

    public static final String TYPE = "elasticsearch";

    private static final String INDEXED_ROW = "indexedRow";
    private static final String REJECTED_ROW = "rejectedRow";
    private static final String PARSING_FAILED = "parsingFailedRow";
    private static final String INDEX_DELAY = "indexDelay";
    private static final String SINK_ID = "sinkId";

    private RestClient client;
    private final List<String> addressList;
    private final Properties ribbonEtc;
    private final IndexInfoBuilder indexInfo;
    private final String clientName;
    private final ObjectMapper jsonMapper;

    public ElasticSearchSink(
        @JsonProperty("clientName") String clientName,
        @JsonProperty("queue4Sink") MessageQueue4Sink queue4Sink,
        @JsonProperty("batchSize") int batchSize,
        @JsonProperty("batchTimeout") int batchTimeout,
        @JsonProperty("addressList") List<String> addressList,
        @JsonProperty("indexInfo") @JacksonInject IndexInfoBuilder indexInfo,
        @JsonProperty("jobQueueSize") int jobQueueSize,
        @JsonProperty("corePoolSize") int corePoolSize,
        @JsonProperty("maxPoolSize") int maxPoolSize,
        @JsonProperty("jobTimeout") long jobTimeout,
        @JsonProperty("ribbon.etc") Properties ribbonEtc,
        @JacksonInject ObjectMapper jsonMapper,
        @JacksonInject RestClient client) {
        super(jobQueueSize, corePoolSize, maxPoolSize, jobTimeout,
            ElasticSearchSink.class.getSimpleName() + "-" + clientName);

        this.indexInfo =
            indexInfo == null ? new DefaultIndexInfoBuilder(null, null, null, null, null, jsonMapper) : indexInfo;

        initialize(
            "es_" + clientName,
            queue4Sink == null ? new MemoryQueue4Sink(10000) : queue4Sink,
            batchSize,
            batchTimeout,
            true);

        this.jsonMapper = jsonMapper;
        this.addressList = addressList;
        this.ribbonEtc = ribbonEtc == null ? new Properties() : ribbonEtc;
        this.clientName = clientName;
        this.client = client;
    }

    @Override
    public void writeTo(MessageContainer message) {
        enqueue(message.getMessage());
    }

    @Override
    public void open() {
        Monitors.registerObject(clientName, this);

        if (client == null) {
            createClient();
        }

        setName(ElasticSearchSink.class.getSimpleName() + "-" + clientName);
        start();
    }

    @Override
    public String recvNotice() { return null; }

    @Override
    public String getStat() {
        StringBuilder sb = new StringBuilder();
        StringBuilder indexDelay = new StringBuilder();
        StringBuilder indexed = new StringBuilder();
        StringBuilder rejected = new StringBuilder();
        StringBuilder parsingFailed = new StringBuilder();

        for (Monitor<?> m : DefaultMonitorRegistry.getInstance().getRegisteredMonitors()) {
            if (m instanceof BasicCounter) {
                BasicCounter counter = (BasicCounter) m;
                String sinkId = counter.getConfig().getTags().getValue(SINK_ID);
                if (!Strings.isNullOrEmpty(sinkId) && sinkId.equals(getSinkId())) {
                    if (counter.getConfig().getName().equals(INDEXED_ROW)) {
                        indexed.append(counter.getConfig().getTags().getValue(TagKey.ROUTING_KEY))
                            .append(":")
                            .append(counter.getValue()).append('\n');
                    } else if (counter.getConfig().getName().equals(REJECTED_ROW)) {
                        rejected.append(counter.getConfig().getTags().getValue(TagKey.ROUTING_KEY))
                            .append(":")
                            .append(counter.getValue()).append('\n');

                    } else if (counter.getConfig().getName().equals(PARSING_FAILED)) {
                        parsingFailed.append(counter.getConfig().getTags().getValue(TagKey.ROUTING_KEY))
                            .append(":")
                            .append(counter.getValue()).append('\n');
                    }
                }
            } else if (m instanceof NumberGauge) {
                NumberGauge gauge = (NumberGauge) m;
                String sinkId = gauge.getConfig().getTags().getValue(SINK_ID);
                if (!Strings.isNullOrEmpty(sinkId) && sinkId.equals(getSinkId())) {
                    if (gauge.getConfig().getName().equals(INDEX_DELAY)) {
                        indexDelay.append(gauge.getConfig().getTags().getValue(TagKey.ROUTING_KEY))
                            .append(":")
                            .append(gauge.getValue()).append('\n');
                    }
                }

            }
        }

        sb.append('\n').append(INDEX_DELAY).append('\n').append(indexDelay.toString());
        sb.append('\n').append(INDEXED_ROW).append('\n').append(indexed.toString());
        sb.append('\n').append(REJECTED_ROW).append('\n').append(rejected.toString());
        sb.append('\n').append(PARSING_FAILED).append('\n').append(parsingFailed.toString());

        return sb.toString();
    }

    @Override
    protected void beforePolling() throws IOException {}

    private void createClient() {
        if (ribbonEtc.containsKey("eureka")) {
            ribbonEtc.setProperty(
                clientName + ".ribbon.AppName", clientName);
            ribbonEtc.setProperty(
                clientName + ".ribbon.NIWSServerListClassName",
                "com.netflix.niws.loadbalancer.DiscoveryEnabledNIWSServerList");
            ribbonEtc.setProperty(
                clientName + ".ribbon.ServerListRefreshInterval",
                "60000");
            String[] host_port = addressList.get(0).split(":");
            ribbonEtc.setProperty(
                clientName + ".ribbon.DeploymentContextBasedVipAddresses",
                host_port[0]);
            ribbonEtc.setProperty(
                clientName + ".ribbon.Port",
                host_port[1]);
        } else {
            ribbonEtc.setProperty(clientName + ".ribbon.listOfServers", Joiner.on(",").join(addressList));
        }
        ConfigurationManager.loadProperties(ribbonEtc);
        client = (RestClient) ClientFactory.getNamedClient(clientName);

    }
    private String createIndexRequest(Message m) {
        IndexInfo info = indexInfo.create(m);
        if (info == null) {
            Servo.getCounter(
                MonitorConfig.builder(PARSING_FAILED)
                    .withTag(SINK_ID, getSinkId())
                    .withTag(TagKey.ROUTING_KEY, m.getRoutingKey())
                    .build()).increment();
            return null;
        } else {
            Servo.getLongGauge(
                MonitorConfig.builder(INDEX_DELAY)
                    .withTag(SINK_ID, getSinkId())
                    .withTag(TagKey.ROUTING_KEY, m.getRoutingKey())
                    .build()).set(System.currentTimeMillis() - info.getTimestamp());

            try {

                StringBuilder sb = new StringBuilder();
                if (info.getId() != null) {
                    sb.append(String.format(
                        "{ \"create\" : { \"_index\" : \"%s\", \"_type\" : \"%s\", \"_id\" : \"%s\" } }",
                        info.getIndex(), info.getType(), info.getId()));
                } else {
                    sb.append(String.format(
                        "{ \"create\" : { \"_index\" : \"%s\", \"_type\" : \"%s\"} }",
                        info.getIndex(), info.getType()));
                }
                sb.append('\n');

                sb.append(getSource(info));
                sb.append('\n');

                return sb.toString();
            } catch (Exception e) {
                Servo.getCounter(
                    MonitorConfig.builder(PARSING_FAILED)
                        .withTag(SINK_ID, getSinkId())
                        .withTag(TagKey.ROUTING_KEY, m.getRoutingKey())
                        .build()).increment();
                return null;
            }
        }
    }

    private String getSource(IndexInfo info) throws JsonProcessingException {
        if (info.getSource() instanceof Map) {
            return jsonMapper.writeValueAsString(info.getSource());
        } else {
            return info.getSource().toString();
        }
    }

    @Override
    protected void write(List<Message> msgList) throws IOException {
        senders.execute(createRunnable(createBulkRequest(msgList)));
    }

    @VisibleForTesting
    protected Pair<HttpRequest, List<Message>> createBulkRequest(List<Message> msgList) {
        List<Message> msgListPayload = new LinkedList<>();
        StringBuilder sb = new StringBuilder();
        for (Message m : msgList) {
            String indexRequest = createIndexRequest(m);
            if (indexRequest != null) {
                sb.append(indexRequest);
                msgListPayload.add(m);
            }
        }

        return new Pair<>(
            HttpRequest.newBuilder()
                .verb(HttpRequest.Verb.POST)
                .uri("/_bulk")
                .entity(sb.toString()).build(),
            msgListPayload);
    }

    private Runnable createRunnable(final Pair<HttpRequest, List<Message>> request) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    HttpResponse response = client.executeWithLoadBalancer(request.first());
                    byte[] bytes = IOUtils.toByteArray(response.getInputStream());
                    Map<String, Object> result = jsonMapper.readValue(bytes, new TypeReference<Map<String, Object>>(){});
                    List items = (List) result.get("items");
                    for (int i = 0; i < items.size(); ++i) {
                        String routingKey = request.second().get(i).getRoutingKey();
                        Map<String, Object> resPerMessage = (Map)((Map)(items.get(i))).get("create");
                        if (isFailed(resPerMessage) && !getErrorMessage(resPerMessage).contains("DocumentAlreadyExistsException")) {
                            log.error("Failed with: " + resPerMessage.get("error"));
                            Servo.getCounter(
                                MonitorConfig.builder(REJECTED_ROW)
                                    .withTag(SINK_ID, getSinkId())
                                    .withTag(TagKey.ROUTING_KEY, routingKey)
                                    .build()).increment();

                            recover(request.second().get(i));
                        } else {
                            Servo.getCounter(
                                MonitorConfig.builder(INDEXED_ROW)
                                    .withTag(SINK_ID, getSinkId())
                                    .withTag(TagKey.ROUTING_KEY, routingKey)
                                    .build()).increment();

                        }
                    }
                    throughput.increment(items.size());
                } catch (Exception e) {
                    log.error("Exception on bulk execute: " + e.getMessage(), e);
                }

            }
        };
    }

    private String getErrorMessage(Map<String, Object> resPerMessage) {
        return (String) resPerMessage.get("error");
    }

    private boolean isFailed(Map<String, Object> resPerMessage) {
        return (int)resPerMessage.get("status") / 100 != 2;
    }

    @VisibleForTesting
    protected void recover(Message message) throws Exception {
        IndexInfo info = indexInfo.create(message);

        String uri = info.getId() != null ?
            String.format(
                "/%s/%s/%s",
                info.getIndex(),
                info.getType(),
                info.getId()) :
            String.format(
                "/%s/%s/",
                info.getIndex(),
                info.getType());

        String entity = info.getSource() instanceof Map ?
            jsonMapper.writeValueAsString(info.getSource()) :
            info.getSource().toString();

        HttpResponse response = client.executeWithLoadBalancer(
            new HttpRequest.Builder()
                .verb(HttpRequest.Verb.POST)
                .uri(uri)
                .entity(entity)
                .build());
        IOUtils.toByteArray(response.getInputStream());
    }

    @Override
    protected void innerClose() {
        super.innerClose();

        client.shutdown();
    }
}
