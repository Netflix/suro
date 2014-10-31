package com.netflix.suro.sink.elasticsearch;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class ElasticSearchSink extends ThreadPoolQueuedSink implements Sink {
    private static Logger log = LoggerFactory.getLogger(ElasticSearchSink.class);

    public static final String TYPE = "elasticsearch";

    private static final String INDEXED_ROW = "indexedRow";
    private static final String REJECTED_ROW = "rejectedRow";
    private static final String PARSING_FAILED = "parsingFailedRow";
    private static final String INDEX_DELAY = "indexDelay";
    private static final String SINK_ID = "sinkId";

    private Client client;
    private final List<String> addressList;
    private final IndexInfoBuilder indexInfo;
    private final DiscoveryClient discoveryClient;
    private final Settings settings;
    private final String clusterName;
    private final ReplicationType replicationType;

    @JsonCreator
    public ElasticSearchSink(
            @JsonProperty("queue4Sink") MessageQueue4Sink queue4Sink,
            @JsonProperty("batchSize") int batchSize,
            @JsonProperty("batchTimeout") int batchTimeout,
            @JsonProperty("cluster.name") String clusterName,
            @JsonProperty("client.transport.sniff") Boolean sniff,
            @JsonProperty("client.transport.ping_timeout") String pingTimeout,
            @JsonProperty("client.transport.nodes_sampler_interval") String nodesSamplerInterval,
            @JsonProperty("addressList") List<String> addressList,
            @JsonProperty("indexInfo") @JacksonInject IndexInfoBuilder indexInfo,
            @JsonProperty("jobQueueSize") int jobQueueSize,
            @JsonProperty("corePoolSize") int corePoolSize,
            @JsonProperty("maxPoolSize") int maxPoolSize,
            @JsonProperty("jobTimeout") long jobTimeout,
            @JsonProperty("pauseOnLongQueue") boolean pauseOnLongQueue,
            @JsonProperty("replicationType") String replicationType,
            @JacksonInject DiscoveryClient discoveryClient,
            @JacksonInject ObjectMapper jsonMapper,
            @JacksonInject Client client) {
        super(jobQueueSize, corePoolSize, maxPoolSize, jobTimeout,
                ElasticSearchSink.class.getSimpleName() + "-" + clusterName);

        this.indexInfo =
                indexInfo == null ? new DefaultIndexInfoBuilder(null, null, null, null, null, jsonMapper) : indexInfo;

        initialize(
                "es_" + clusterName,
                queue4Sink == null ? new MemoryQueue4Sink(10000) : queue4Sink,
                batchSize,
                batchTimeout,
                pauseOnLongQueue);

        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        if (clusterName != null) {
            settingsBuilder = settingsBuilder.put("cluster.name", clusterName);
        } else {
            clusterName = "NA";
            settingsBuilder = settingsBuilder.put("client.transport.ignore_cluster_name", true);
        }

        if (sniff != null) {
            settingsBuilder = settingsBuilder.put("client.transport.sniff", sniff);
        }
        if (pingTimeout != null) {
            settingsBuilder = settingsBuilder.put("client.transport.ping_timeout", pingTimeout);
        }
        if (nodesSamplerInterval != null) {
            settingsBuilder = settingsBuilder.put("client.transport.nodes_sampler_interval", nodesSamplerInterval);
        }

        this.client = client;
        this.settings = settingsBuilder.build();
        this.discoveryClient = discoveryClient;
        this.addressList = addressList;
        this.clusterName = clusterName;
        this.replicationType = replicationType == null ? ReplicationType.ASYNC : ReplicationType.fromString(replicationType);
    }

    @Override
    public void writeTo(MessageContainer message) {
        enqueue(message.getMessage());
    }

    @Override
    public void open() {
        Monitors.registerObject(clusterName, this);

        if (client == null) {
            client = new TransportClient(settings);
            if (discoveryClient != null) {
                getServerListFromDiscovery(addressList, discoveryClient, client);
            } else {
                for (String address : addressList) {
                    String[] host_port = address.split(":");
                    ((TransportClient)client).addTransportAddress(
                            new InetSocketTransportAddress(host_port[0], Integer.parseInt(host_port[1])));
                }
            }
        }

        setName(ElasticSearchSink.class.getSimpleName() + "-" + settings.get("cluster.name"));
        start();
    }

    protected void getServerListFromDiscovery(List<String> addressList, DiscoveryClient discoveryClient, Client client) {
        for (String address : addressList) {
            String[] host_port = address.split(":");

            List<InstanceInfo> listOfinstanceInfo = discoveryClient.getInstancesByVipAddress(host_port[0], false);
            for (InstanceInfo ii : listOfinstanceInfo) {
                if (ii.getStatus().equals(InstanceInfo.InstanceStatus.UP)) {
                    ((TransportClient)client).addTransportAddress(
                            new InetSocketTransportAddress(ii.getHostName(), Integer.parseInt(host_port[1])));
                }
            }
        }
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

    private IndexRequest createIndexRequest(Message m) {
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

            return Requests.indexRequest(info.getIndex())
                            .type(info.getType())
                            .source(info.getSource())
                            .id(info.getId())
                            .replicationType(replicationType)
                            .opType(IndexRequest.OpType.CREATE);
        }
    }

    @Override
    protected void write(List<Message> msgList) throws IOException {
        final BulkRequest request = createBulkRequest(msgList);

        senders.execute(createRunnable(request));
    }

    @VisibleForTesting
    protected BulkRequest createBulkRequest(List<Message> msgList) {
        final BulkRequest request = new BulkRequest();
        for (Message m : msgList) {
            IndexRequest indexRequest = createIndexRequest(m);
            if (indexRequest != null) {
                request.add(indexRequest, m);
            }
        }
        return request;
    }

    private Runnable createRunnable(final BulkRequest request) {
        return new Runnable() {
            @Override
            public void run() {
                BulkResponse response = client.bulk(request).actionGet();
                for (BulkItemResponse r : response.getItems()) {
                    String routingKey = ((Message) request.payloads().get(r.getItemId())).getRoutingKey();
                    if (r.isFailed() && !r.getFailureMessage().contains("DocumentAlreadyExistsException")) {
                        log.error("Failed with: " + r.getFailureMessage());
                        Servo.getCounter(
                                MonitorConfig.builder(REJECTED_ROW)
                                        .withTag(SINK_ID, getSinkId())
                                        .withTag(TagKey.ROUTING_KEY, routingKey)
                                        .build()).increment();

                        recover(r.getItemId(), request);
                    } else {
                        Servo.getCounter(
                                MonitorConfig.builder(INDEXED_ROW)
                                        .withTag(SINK_ID, getSinkId())
                                        .withTag(TagKey.ROUTING_KEY, routingKey)
                                        .build()).increment();

                    }
                }

                throughput.increment(response.getItems().length);
            }
        };
    }

    @VisibleForTesting
    protected void recover(int itemId, BulkRequest request) {
        client.index(createIndexRequest((Message) request.payloads().get(itemId))).actionGet();
    }

    @Override
    protected void innerClose() {
        super.innerClose();

        client.close();
    }
}
