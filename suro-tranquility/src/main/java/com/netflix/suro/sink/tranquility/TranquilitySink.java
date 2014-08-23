package com.netflix.suro.sink.tranquility;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.druid.*;
import com.metamx.tranquility.typeclass.Timestamper;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.queue.MemoryQueue4Sink;
import com.netflix.suro.queue.MessageQueue4Sink;
import com.netflix.suro.sink.DataConverter;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.ThreadPoolQueuedSink;
import com.twitter.finagle.Service;
import io.druid.curator.PotentiallyGzippedCompressionProvider;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class TranquilitySink extends ThreadPoolQueuedSink implements Sink {
    private static Logger log = LoggerFactory.getLogger(TranquilitySink.class);

    private final static TypeReference<Map<String, Object>> type = new TypeReference<Map<String, Object>>() {};

    public static final String TYPE = "tranquility";

    private Service<List<Map<String, Object>>, Integer> druidService;
    private final String dataSource;
    private final String discoveryPath;
    private final TimestampSpec timestampSpec;
    private final String indexServiceName;
    private final String firehosePattern;
    private final List<String> dimensions;
    private final List<AggregatorFactory> aggregators;
    private final QueryGranularity indexGranularity;
    private final Granularity segmentGranularity;
    private final Period warmingPeriod;
    private final Period windowPeriod;
    private final int partitions;
    private final int replicants;

    private final CuratorFramework curator;
    private final ObjectMapper jsonMapper;
    private final DataConverter dataConverter;

    @Monitor(name = "sentMessages", type = DataSourceType.COUNTER)
    private AtomicLong sentMessages = new AtomicLong(0);

    @JsonCreator
    public TranquilitySink(
            @JsonProperty("queue4Sink") MessageQueue4Sink queue4Sink,
            @JsonProperty("batchSize") int batchSize,
            @JsonProperty("batchTimeout") int batchTimeout,
            @JsonProperty("jobQueueSize") int jobQueueSize,
            @JsonProperty("corePoolSize") int corePoolSize,
            @JsonProperty("maxPoolSize") int maxPoolSize,
            @JsonProperty("jobTimeout") long jobTimeout,
            @JsonProperty("dataSource") String dataSource,
            @JsonProperty("discoveryPath") String discoveryPath,
            @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
            @JsonProperty("indexServiceName") String indexServiceName,
            @JsonProperty("firehosePattern") String firehosePattern,
            @JsonProperty("dimensions") List<String> dimensions,
            @JsonProperty("aggregators") List<AggregatorFactory> aggregators,
            @JsonProperty("indexGranularity") QueryGranularity indexGranularity,
            @JsonProperty("segmentGranularity") Granularity segmentGranularity,
            @JsonProperty("warmingPeriod") Period warmingPeriod,
            @JsonProperty("windowPeriod") Period windowPeriod,
            @JsonProperty("partitions") int partitions,
            @JsonProperty("replicants") int replicants,
            @JsonProperty("druid.zk.service.host") @JacksonInject("druid.zk.service.host") String zkServiceHost,
            @JsonProperty("druid.zk.service.sessionTimeoutMs") int zkSessionTimeoutMs,
            @JsonProperty("druid.curator.compress") boolean zkCompress,
            @JacksonInject ObjectMapper jsonMapper,
            @JacksonInject DataConverter dataConverter) {
        super(jobQueueSize, corePoolSize, maxPoolSize, jobTimeout, TranquilitySink.class.getSimpleName() + "-" + dataSource);

        Preconditions.checkNotNull(dataSource);
        Preconditions.checkNotNull(dimensions);
        Preconditions.checkNotNull(jsonMapper);
        Preconditions.checkNotNull(zkServiceHost);

        this.dataSource = dataSource;
        this.discoveryPath = discoveryPath == null ? "/druid/discovery" : discoveryPath;
        this.timestampSpec = timestampSpec == null ? new TimestampSpec("ts", "millis") : timestampSpec;
        this.indexServiceName = indexServiceName == null ? "druid:overlord" : indexServiceName;
        this.firehosePattern = firehosePattern == null ? "druid:firehose:%s" : firehosePattern;
        this.dimensions = dimensions;
        this.aggregators = aggregators == null ? getDefaultAggregators() : aggregators;
        this.indexGranularity = indexGranularity == null ? QueryGranularity.MINUTE : indexGranularity;
        this.segmentGranularity = segmentGranularity == null ? Granularity.HOUR : segmentGranularity;
        this.warmingPeriod = warmingPeriod == null ? new Period("PT0M") : warmingPeriod;
        this.windowPeriod = windowPeriod == null ? new Period("PT10M") : windowPeriod;
        this.partitions = partitions == 0 ? 1 : partitions;
        this.replicants = replicants == 0 ? 1 : replicants;

        this.curator = createCurator(zkServiceHost, zkSessionTimeoutMs, zkCompress);
        this.jsonMapper = jsonMapper;
        this.dataConverter = dataConverter;

        initialize("tranq_" + dataSource, queue4Sink == null ? new MemoryQueue4Sink(10000) : queue4Sink, batchSize, batchTimeout);
    }

    private CuratorFramework createCurator(String zkServiceHost, int zkSessionTimeoutMs, boolean zkCompress) {
        Preconditions.checkNotNull(zkServiceHost);

        CuratorFramework curator = CuratorFrameworkFactory.builder()
                .connectString(zkServiceHost)
                .sessionTimeoutMs(zkSessionTimeoutMs == 0 ? 30000 : zkSessionTimeoutMs)
                .retryPolicy(new BoundedExponentialBackoffRetry(1000, 45000, 30))
                .compressionProvider(new PotentiallyGzippedCompressionProvider(zkCompress))
                .build();
        curator.start();

        return curator;
    }

    @Override
    protected void beforePolling() throws IOException {}

    @Override
    protected void write(List<Message> msgList) throws IOException {
        final List<Map<String, Object>> msgMapList = new ArrayList<Map<String, Object>>();

        for (Message m : msgList) {
            try {
                final Map<String, Object> msgMap;
                if (dataConverter != null) {
                    msgMap = dataConverter.convert((Map<String, Object>) jsonMapper.readValue(m.getPayload(), type));
                } else {
                    msgMap = jsonMapper.readValue(m.getPayload(), type);
                }
                msgMapList.add(msgMap);
            } catch (Exception e) {
                log.error("Parsing failed", e);
            }
        }

        senders.submit(new Runnable() {
            @Override
            public void run() {
                sentMessages.addAndGet(druidService.apply(msgMapList).get());
            }
        });
    }

    @Override
    public void writeTo(MessageContainer message) {
        enqueue(message.getMessage());
    }

    @Override
    public void open() {
        druidService = DruidBeams
                .builder(
                        new Timestamper<Map<String, Object>>() {
                            @Override
                            public DateTime timestamp(Map<String, Object> theMap) {
                                return new DateTime(theMap.get(timestampSpec.getTimestampColumn()));
                            }
                        }
                )
                .curator(curator)
                .discoveryPath(discoveryPath)
                .location(
                        new DruidLocation(
                                new DruidEnvironment(
                                        indexServiceName,
                                        firehosePattern
                                ), dataSource
                        )
                )
                .rollup(DruidRollup.create(DruidDimensions.specific(dimensions), aggregators, indexGranularity))
                .tuning(ClusteredBeamTuning.create(segmentGranularity, warmingPeriod, windowPeriod, partitions, replicants))
                .timestampSpec(timestampSpec)
                .buildJavaService();

        Monitors.registerObject(dataSource, this);

        start();
    }

    @Override
    public String recvNotice() { return null; }

    @Override
    public String getStat() {
        return sentMessages.get() + " sent";
    }

    @Override
    protected void innerClose() {
        super.innerClose();

        druidService.close().get();
    }

    public List<AggregatorFactory> getDefaultAggregators() {
        return ImmutableList.<AggregatorFactory>of(
                new CountAggregatorFactory(
                        "count"
                )
        );
    }
}
