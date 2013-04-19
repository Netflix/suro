package com.netflix.suro.queue;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.Monitors;
import com.netflix.suro.TagKey;
import com.netflix.suro.message.MessageSetBuilder;
import com.netflix.suro.message.MessageSetReader;
import com.netflix.suro.server.ServerConfig;
import com.netflix.suro.thrift.Result;
import com.netflix.suro.thrift.ResultCode;
import com.netflix.suro.thrift.TMessageSet;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@Singleton
public class MemoryMessageQueue extends MessageQueue {
    static Logger log = LoggerFactory.getLogger(MemoryMessageQueue.class);

    private volatile boolean isRunning = false;

    private final BlockingQueue<TMessageSet> queue;
    private final QueueManager manager;

    @Inject
    public MemoryMessageQueue(ServerConfig config, QueueManager manager) throws Exception {
        queue = new LinkedBlockingDeque<TMessageSet>(config.getMemoryQueueSize());
        this.manager = manager;
        isRunning = true;

        manager.registerService(this);
        Monitors.registerObject(this);
    }

    @Monitor(name ="QueueSize", type= DataSourceType.GAUGE)
    public int getQueueSize() {
        return queue.size();
    }

    private static final String messageSetCountMetric = "messageSetCount";
    @Monitor(name= messageSetCountMetric, type=DataSourceType.COUNTER)
    private long messageSetCount;

    private static final String retryCountMetric = "retryCount";
    @Monitor(name=retryCountMetric, type=DataSourceType.COUNTER)
    private long retryCount;

    private static final String dataCorruptionCountMetric = "retryCount";
    @Monitor(name="dataCorruption", type=DataSourceType.COUNTER)
    private long dataCorruption;

    @Override
    public String getName() throws TException {
        return "Suro-MemoryMessageQueue";
    }

    @Override
    public String getVersion() throws TException {
        return "V1.0.0";
    }

    @Override
    public Result process(TMessageSet messageSet) throws TException {
        Result result = new Result();
        try {
            // Stop adding chunks if it's no running
            if (!isRunning) {
                log.warn("Rejecting some incoming trafic!");
                result.setMessage("Shutting down");
                result.setResultCode(ResultCode.STOPPED);
                return result;
            }

            if (!isTakingTraffic) {
                log.warn("Rejecting some incoming trafic! - >>>>>>> Flag is ON <<<<<<< ");
                result.setMessage("collector in error");
                result.setResultCode(ResultCode.OTHER_ERROR);
                return result;
            }

            MessageSetReader reader = new MessageSetReader(messageSet);
            if (reader.checkCRC() == false) {
                ++dataCorruption;

                DynamicCounter.increment(dataCorruptionCountMetric, TagKey.APP, messageSet.getApp());

                result.setMessage("data corrupted");
                result.setResultCode(ResultCode.CRC_CORRUPTED);
                return result;
            }

            if (queue.offer(messageSet)) {
                ++messageSetCount;

                DynamicCounter.increment(messageSetCountMetric, TagKey.APP, messageSet.getApp());

                result.setMessage(Long.toString(messageSet.getCrc()));
                result.setResultCode(ResultCode.OK);
            } else {
                ++retryCount;

                DynamicCounter.increment(retryCountMetric, TagKey.APP, messageSet.getApp());

                result.setMessage(Long.toString(messageSet.getCrc()));
                result.setResultCode(ResultCode.QUEUE_FULL);
            }

            return result;
        } catch (OutOfMemoryError oom) {
            log.error("OutOfMemoryError: " + oom.getLocalizedMessage(), oom);
            oom.printStackTrace();
            System.exit(-1);
        } catch (Throwable e) {
            log.error("Throable handled: " + e.getMessage(), e);
        }

        return result;
    }

    @Override
    public long shutdown() throws TException {
        isRunning = false;
        return 0;
    }


    @Override
    public TMessageSet poll(long timeout, TimeUnit unit) {
        try {
            return queue.poll(timeout, unit);
        } catch (InterruptedException e) {
            // return empty payload
            return new MessageSetBuilder().build();
        }
    }
}
