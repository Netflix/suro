package com.netflix.suro.sink;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class ThreadPoolQueuedSink extends QueuedSink {
    protected final ThreadPoolExecutor senders;
    protected final ArrayBlockingQueue<Runnable> jobQueue;
    protected final long jobTimeout;

    @Monitor(name = "jobQueueSize", type = DataSourceType.GAUGE)
    public long getJobQueueSize() {
        return jobQueue.size();
    }

    public ThreadPoolQueuedSink(
            int jobQueueSize,
            int corePoolSize,
            int maxPoolSize,
            long jobTimeout,
            String threadFactoryName) {
        jobQueue = new ArrayBlockingQueue<Runnable>(jobQueueSize == 0 ? 100 : jobQueueSize) {
            @Override
            public boolean offer(Runnable runnable) {
                try {
                    put(runnable); // not to reject the task, slowing down
                } catch (InterruptedException e) {
                    // do nothing
                }
                return true;
            }
        };
        senders = new ThreadPoolExecutor(
                corePoolSize == 0 ? 3 : corePoolSize,
                maxPoolSize == 0 ? 10 : maxPoolSize,
                10, TimeUnit.SECONDS,
                jobQueue,
                new ThreadFactoryBuilder().setNameFormat(threadFactoryName + "-Sender-%d").build());
        this.jobTimeout = jobTimeout;
    }

    @Override
    protected void innerClose() {
        senders.shutdown();
        try {
            senders.awaitTermination(jobTimeout == 0 ? Long.MAX_VALUE : jobTimeout, TimeUnit.MILLISECONDS);
            if (!senders.isTerminated()) {
                log.error(this.getClass().getSimpleName() + " not terminated gracefully");
            }
        } catch (InterruptedException e) {
            log.error("Interrupted: " + e.getMessage());
        }
    }

    @Override
    public long getNumOfPendingMessages() {
        long messagesInQueue = super.getNumOfPendingMessages();
        // add messages in thread pool, either in job queue or active
        return messagesInQueue + getJobQueueSize() + senders.getActiveCount();
    }
}
