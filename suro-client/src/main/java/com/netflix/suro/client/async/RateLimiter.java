/*
 * Copyright 2013 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.suro.client.async;

/**
 * A simple rate limiter based on <a href="http://lucene.apache.org/">Lucene</a>'s RateLimiter.SimpleRateLimiter.
 */
public class RateLimiter {
    private volatile int msgPerSec;
    private volatile double nsPerMsg;
    private volatile long lastNS;

    public RateLimiter(int msgPerSec) {
        setMsgPerSec(msgPerSec);
    }

    public void setMsgPerSec(int msgPerSec) {
        this.msgPerSec = msgPerSec;
        nsPerMsg = 1000000000. / msgPerSec;

    }

    public int getMsgPerSec() {
        return this.msgPerSec;
    }

    /** Pauses, if necessary, to keep the instantaneous IO
     *  rate at or below the target. NOTE: multiple threads
     *  may safely use this, however the implementation is
     *  not perfectly thread safe but likely in practice this
     *  is harmless (just means in some rare cases the rate
     *  might exceed the target).  It's best to call this
     *  with a biggish count, not one byte at a time.
     *  @return the pause time in nano seconds
     * */
    public long pause(int msgs) throws InterruptedException {
        if (msgs == 1) {
            return 0;
        }

        // TODO: this is purely instantaneous rate; maybe we
        // should also offer decayed recent history one?
        final long targetNS = lastNS = lastNS + ((long) (msgs * nsPerMsg));
        final long startNS;
        long curNS = startNS = System.nanoTime();
        if (lastNS < curNS) {
            lastNS = curNS;
        }

        // While loop because Thread.sleep doesn't always sleep
        // enough:
        while(true) {
            final long pauseNS = targetNS - curNS;
            if (pauseNS > 0) {
                Thread.sleep((int) (pauseNS/1000000), (int) (pauseNS % 1000000));
                curNS = System.nanoTime();
                continue;
            }
            break;
        }
        return curNS - startNS;
    }
}
