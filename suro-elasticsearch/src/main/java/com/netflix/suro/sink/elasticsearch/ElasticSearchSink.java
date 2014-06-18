package com.netflix.suro.sink.elasticsearch;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.queue.MemoryQueue4Sink;
import com.netflix.suro.queue.MessageQueue4Sink;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.ThreadPoolQueuedSink;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
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

    private Client client;
    private final List<String> addressList;
    private final IndexInfoBuilder indexInfo;
    private final DiscoveryClient discoveryClient;
    private final Settings settings;

    @Monitor(name = "indexedRowCount", type = DataSourceType.COUNTER)
    private long indexedRowCount = 0;

    @Monitor(name = "parsingFailedRowCount", type = DataSourceType.COUNTER)
    private long parsingFailedRowCount = 0;

    @Monitor(name = "rejectedRowCount", type = DataSourceType.COUNTER)
    private long rejectedRowCount = 0;

    @Monitor(name = "indexDelay", type = DataSourceType.GAUGE)
    private long indexDelay = 0;

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
            @JacksonInject DiscoveryClient discoveryClient,
            @JacksonInject ObjectMapper jsonMapper,
            @JacksonInject Client client) {
        super(jobQueueSize, corePoolSize, maxPoolSize, jobTimeout,
                ElasticSearchSink.class.getSimpleName() + "-" + clusterName);

        this.indexInfo =
                indexInfo == null ? new DefaultIndexInfoBuilder(null, null, null, null, null, jsonMapper) : indexInfo;

        initialize(queue4Sink == null ? new MemoryQueue4Sink(10000) : queue4Sink, batchSize, batchTimeout);

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

        Monitors.registerObject(clusterName, this);
    }

    @Override
    public void writeTo(MessageContainer message) {
        enqueue(message.getMessage());
    }

    @Override
    public void open() {
        if (client == null) {
            client = new TransportClient(settings);
            if (discoveryClient != null) {
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

    @Override
    public String recvNotice() { return null; }

    @Override
    public String getStat() {
        return String.format("indexed: %d, rejected: %d, parsing failed: %d",
                indexedRowCount, rejectedRowCount, parsingFailedRowCount);
    }

    @Override
    protected void beforePolling() throws IOException {}

    @Override
    protected void write(List<Message> msgList) throws IOException {
        final BulkRequest request = new BulkRequest();
        for (Message m : msgList) {
            IndexInfo info = indexInfo.create(m);
            if (info == null) {
                ++parsingFailedRowCount;
            } else {
                request.add(Requests.indexRequest(info.getIndex())
                        .type(info.getType())
                        .source(info.getSource())
                        .id(info.getId())
                        .opType(IndexRequest.OpType.CREATE),
                        m);
            }
        }

        senders.execute(createRunnable(request));
    }

    private Runnable createRunnable(final BulkRequest request) {
        return new Runnable() {
            @Override
            public void run() {
                BulkResponse response = client.bulk(request).actionGet();
                if (response.hasFailures()) {
                    int rejectedCount = 0;
                    for (BulkItemResponse r : response.getItems()) {
                        if (r.isFailed() && !r.getFailureMessage().contains("DocumentAlreadyExistsException")) {
                            log.error("Failed with: " + r.getFailureMessage());
                            ++rejectedCount;

                            enqueue((Message) request.payloads().get(r.getItemId()));
                        }
                    }
                    rejectedRowCount += rejectedCount;
                    indexedRowCount += request.numberOfActions() - rejectedCount;
                } else {
                    indexedRowCount += request.numberOfActions();
                }

                indexDelay = System.currentTimeMillis() - indexInfo.create((Message) request.payloads().get(0)).getTimestamp();
            }
        };
    }

    @Override
    protected void innerClose() {
        super.innerClose();

        client.close();
    }
}
