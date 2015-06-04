package com.netflix.suro.sink.elasticsearch;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.netflix.client.ClientFactory;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.http.HttpRequest;
import com.netflix.client.http.HttpResponse;
import com.netflix.config.ConfigurationManager;
import com.netflix.niws.client.http.RestClient;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.*;
import com.netflix.suro.TagKey;
import com.netflix.suro.message.DefaultMessageContainer;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.queue.MemoryQueue4Sink;
import com.netflix.suro.queue.MessageQueue4Sink;
import com.netflix.suro.servo.Servo;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.ThreadPoolQueuedSink;
import com.netflix.util.Pair;
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
    private static final String ABANDONED_MESSAGES_ON_EXCEPTION = "abandonedMessagesOnException";

    private RestClient client;
    private final List<String> addressList;
    private final Properties ribbonEtc;
    private final IndexInfoBuilder indexInfo;
    private final String clientName;
    private final ObjectMapper jsonMapper;
    private final Timer timer;
    private final int sleepOverClientException;
    private final boolean reEnqueueOnException;

    public static final class BatchProcessingException extends Exception {
        public BatchProcessingException(String message) {
            super(message);
        }
    }

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
        @JsonProperty("sleepOverClientException") int sleepOverClientException,
        @JsonProperty("ribbon.etc") Properties ribbonEtc,
        @JsonProperty("reEnqueueOnException") boolean reEnqueueOnException,
        @JacksonInject ObjectMapper jsonMapper,
        @JacksonInject RestClient client) {
        super(jobQueueSize, corePoolSize, maxPoolSize, jobTimeout, clientName);

        this.indexInfo =
            indexInfo == null ? new DefaultIndexInfoBuilder(null, null, null, null, null, jsonMapper) : indexInfo;

        initialize(
            clientName,
            queue4Sink == null ? new MemoryQueue4Sink(10000) : queue4Sink,
            batchSize,
            batchTimeout,
            true);

        this.jsonMapper = jsonMapper;
        this.addressList = addressList;
        this.ribbonEtc = ribbonEtc == null ? new Properties() : ribbonEtc;
        this.clientName = clientName;
        this.client = client;
        this.timer = Servo.getTimer(clientName + "_latency");
        this.sleepOverClientException = sleepOverClientException;
        this.reEnqueueOnException = reEnqueueOnException;
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

        setName(clientName);
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

    @VisibleForTesting
    void createClient() {
        if (ribbonEtc.containsKey("eureka")) {
            ribbonEtc.setProperty(
                clientName + ".ribbon.AppName", clientName);
            ribbonEtc.setProperty(
                clientName + ".ribbon.NIWSServerListClassName",
                "com.netflix.niws.loadbalancer.DiscoveryEnabledNIWSServerList");
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
        ribbonEtc.setProperty(
            clientName + ".ribbon.EnablePrimeConnections",
            "true");
        String retryPropertyName = clientName + ".ribbon." + CommonClientConfigKey.OkToRetryOnAllOperations;
        if (ribbonEtc.getProperty(retryPropertyName) == null) {
            // default set this to enable retry on POST operation upon read timeout
            ribbonEtc.setProperty(retryPropertyName, "true");
        }
        String maxRetryProperty = clientName + ".ribbon." + CommonClientConfigKey.MaxAutoRetriesNextServer;
        if (ribbonEtc.getProperty(maxRetryProperty) == null) {
            // by default retry two different servers upon exception
            ribbonEtc.setProperty(maxRetryProperty, "2");
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
                sb.append(indexInfo.getActionMetadata(info));
                sb.append('\n');

                sb.append(indexInfo.getSource(info));
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
                .setRetriable(true)
                .entity(sb.toString()).build(),
            msgListPayload);
    }

    private Runnable createRunnable(final Pair<HttpRequest, List<Message>> request) {
        return new Runnable() {
            @Override
            public void run() {
                Stopwatch stopwatch = timer.start();
                HttpResponse response = null;
                List items = null;
                try {
                    response = client.executeWithLoadBalancer(request.first());
                    stopwatch.stop();
                    if (response.getStatus() / 100 == 2) {
                        Map<String, Object> result = jsonMapper.readValue(
                                response.getInputStream(),
                                new TypeReference<Map<String, Object>>() {
                                });
                        log.debug("Response from ES: {}", result);
                        items = (List) result.get("items");
                        if (items == null || items.size() == 0) {
                            throw new BatchProcessingException("No items in the response");
                        }
                        throughput.increment(items.size());
                    } else {
                        throw new BatchProcessingException("Status is " + response.getStatus());
                    }
                } catch (Exception e) {
                    // Handling the exception on the batch request here
                    log.error("Exception on bulk execute: " + e.getMessage(), e);
                    Servo.getCounter("bulkException").increment();
                    if (reEnqueueOnException) {
                        for (Message m : request.second()) {
                            writeTo(new DefaultMessageContainer(m, jsonMapper));
                        }
                        if (sleepOverClientException > 0) {
                            // sleep on exception for not pushing too much stress
                            try {
                                Thread.sleep(sleepOverClientException);
                            } catch (InterruptedException e1) {
                                // do nothing
                            }
                        }
                    } else {
                        for (Message message: request.second()) {
                            recover(message);
                        }
                    }
                } finally {
                    if (response != null) {
                        response.close();
                    }
                }
                if (items != null) {
                    for (int i = 0; i < items.size(); ++i) {
                        String routingKey = request.second().get(i).getRoutingKey();
                        Map<String, Object> resPerMessage = null;
                        try {
                            resPerMessage = (Map) ((Map) (items.get(i))).get(indexInfo.getCommand());
                        } catch (Exception e) {
                            // could be NPE or cast exception in case the response is unexpected
                            log.error("Unexpected exception", e);
                        }
                        if (resPerMessage == null ||
                                (isFailed(resPerMessage) && !getErrorMessage(resPerMessage).contains("DocumentAlreadyExistsException"))) {
                            if (resPerMessage != null) {
                                log.error("Failed indexing event " + routingKey + " with error message: " + resPerMessage.get("error"));
                            } else {
                                log.error("Response for event " + routingKey + " is null. Request is " + request.second().get(i));
                            }
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
                }
            }
        };
    }

    private String getErrorMessage(Map<String, Object> resPerMessage) {
        return (String) resPerMessage.get("error");
    }

    private boolean isFailed(Map<String, Object> resPerMessage) {
        if (resPerMessage != null) {
            return (int) resPerMessage.get("status") / 100 != 2;
        } else {
            return true;
        }
    }

    public void recover(Message message) {
        IndexInfo info = indexInfo.create(message);

        HttpResponse response = null;
        try {
            response = client.executeWithLoadBalancer(
                HttpRequest.newBuilder()
                    .verb(HttpRequest.Verb.POST)
                    .setRetriable(true)
                    .uri(indexInfo.getIndexUri(info))
                    .entity(indexInfo.getSource(info))
                    .build());
            if (response.getStatus() / 100 != 2) {
                Servo.getCounter(
                    MonitorConfig.builder("unrecoverableRow")
                        .withTag(SINK_ID, getSinkId())
                        .withTag(TagKey.ROUTING_KEY, message.getRoutingKey())
                        .build()).increment();
            }
        } catch (Exception e) {
            log.error("Exception while recover: " + e.getMessage(), e);
            Servo.getCounter("recoverException").increment();
            if (reEnqueueOnException) {
                writeTo(new DefaultMessageContainer(message, jsonMapper));
            } else {
                Servo.getCounter(
                        MonitorConfig.builder("unrecoverableRow")
                                .withTag(SINK_ID, getSinkId())
                                .withTag(TagKey.ROUTING_KEY, message.getRoutingKey())
                                .build()).increment();
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }

    }

    @Override
    protected void innerClose() {
        super.innerClose();

        client.shutdown();
    }

    @VisibleForTesting
    RestClient getClient() {
        return client;
    }

    @VisibleForTesting
    int getSleepOverClientException() {
        return sleepOverClientException;
    }

    @VisibleForTesting
    boolean getReenqueueOnException() {
        return reEnqueueOnException;
    }
}
