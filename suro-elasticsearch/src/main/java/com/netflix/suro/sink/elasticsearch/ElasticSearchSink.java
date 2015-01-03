package com.netflix.suro.sink.elasticsearch;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.DiscoveryClient;
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
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.params.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ElasticSearchSink extends ThreadPoolQueuedSink implements Sink {
    private static Logger log = LoggerFactory.getLogger(ElasticSearchSink.class);

    public static final String TYPE = "elasticsearch";

    private static final String INDEXED_ROW = "indexedRow";
    private static final String REJECTED_ROW = "rejectedRow";
    private static final String PARSING_FAILED = "parsingFailedRow";
    private static final String INDEX_DELAY = "indexDelay";
    private static final String SINK_ID = "sinkId";

    private JestClient client;
    private final List<String> addressList;
    private final IndexInfoBuilder indexInfo;
    private final DiscoveryClient discoveryClient;
    private final String clusterName;

    private ScheduledExecutorService refreshServerList;

    public ElasticSearchSink(
        @JsonProperty("queue4Sink") MessageQueue4Sink queue4Sink,
        @JsonProperty("batchSize") int batchSize,
        @JsonProperty("batchTimeout") int batchTimeout,
        @JsonProperty("cluster.name") String clusterName,
        @JsonProperty("addressList") List<String> addressList,
        @JsonProperty("indexInfo") @JacksonInject IndexInfoBuilder indexInfo,
        @JsonProperty("jobQueueSize") int jobQueueSize,
        @JsonProperty("corePoolSize") int corePoolSize,
        @JsonProperty("maxPoolSize") int maxPoolSize,
        @JsonProperty("jobTimeout") long jobTimeout,
        @JacksonInject DiscoveryClient discoveryClient,
        @JacksonInject ObjectMapper jsonMapper) {
        super(jobQueueSize, corePoolSize, maxPoolSize, jobTimeout,
            ElasticSearchSink.class.getSimpleName() + "-" + clusterName);

        this.indexInfo =
            indexInfo == null ? new DefaultIndexInfoBuilder(null, null, null, null, null, jsonMapper) : indexInfo;

        initialize(
            "es_" + clusterName,
            queue4Sink == null ? new MemoryQueue4Sink(10000) : queue4Sink,
            batchSize,
            batchTimeout,
            true);

        this.discoveryClient = discoveryClient;
        this.addressList = addressList;
        this.clusterName = clusterName;
    }

    protected List<String> getServerListFromDiscovery(List<String> addressList, DiscoveryClient discoveryClient) {
        List<String> serverList = new ArrayList<>();

        for (String address : addressList) {
            String[] host_port = address.split(":");

            List<InstanceInfo> listOfinstanceInfo = discoveryClient.getApplication(host_port[0]).getInstances();
            for (InstanceInfo ii : listOfinstanceInfo) {
                if (ii.getASGName().endsWith("_data") && ii.getStatus().equals(InstanceInfo.InstanceStatus.UP)) {
                    serverList.add(ii.getHostName() + ":" + host_port[1]);
                }
            }
        }

        return serverList;
    }

    @VisibleForTesting
    protected int refreshIntervalInSec = 60;

    @Override
    public void writeTo(MessageContainer message) {
        enqueue(message.getMessage());
    }

    // for the testing purpose only
    public void setClient(JestClient client) {
        this.client = client;
    }

    @Override
    public void open() {
        Monitors.registerObject(clusterName, this);

        List<String> urls = new ArrayList<String>();
        if (discoveryClient != null) {
            for (String s : getServerListFromDiscovery(addressList, discoveryClient)) {
                urls.add(s);
            }
        } else {
            for (String address : addressList) {
                urls.add(address);
            }
        }

        if (client == null) {
            JestClientFactory factory = new JestClientFactory();
            factory.setHttpClientConfig(new HttpClientConfig
                .Builder(urls)
                .multiThreaded(true)
                .build());
            client = factory.getObject();
        }

        String sinkId = ElasticSearchSink.class.getSimpleName() + "-" + clusterName;
        if (discoveryClient != null) {
            refreshServerList = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setDaemon(false).setNameFormat(sinkId + "-%d").build());
            refreshServerList.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    client.setServers(Sets.newHashSet(getServerListFromDiscovery(addressList, discoveryClient)));
                }
            }, refreshIntervalInSec, refreshIntervalInSec, TimeUnit.SECONDS); // every minute refresh
        }

        setName(sinkId);
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

    private BulkableAction createIndexRequest(Message m) {
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

            return new Index.Builder(info.getSource())
                .index(info.getIndex())
                .type(info.getType())
                .id(info.getId())
                .setParameter(Parameters.OP_TYPE, "create")
                .build();
        }
    }

    @Override
    protected void write(List<Message> msgList) throws IOException {
        final Pair<Bulk, List<Message>> request = createBulkRequest(msgList);

        senders.execute(createRunnable(request));
    }

    @VisibleForTesting
    protected Pair<Bulk, List<Message>> createBulkRequest(List<Message> msgList) {
        List<Message> msgListPayload = new LinkedList<>();
        Bulk.Builder builder = new Bulk.Builder();
        for (Message m : msgList) {
            BulkableAction indexRequest = createIndexRequest(m);
            if (indexRequest != null) {
                builder.addAction(indexRequest);
                msgListPayload.add(m);
            }
        }
        return new Pair<>(builder.build(), msgListPayload);
    }

    private Runnable createRunnable(final Pair<Bulk, List<Message>> request) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    JestResult response = client.execute(request.first());
                    List items = (List) response.getValue("items");
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
        return (int)((Double) resPerMessage.get("status") / 100) != 2;
    }

    @VisibleForTesting
    protected void recover(Message message) throws Exception {
        BulkableAction request = createIndexRequest(message);
        client.execute(request);
    }

    @Override
    protected void innerClose() {
        super.innerClose();

        client.shutdownClient();
    }
}
