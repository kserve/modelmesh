/*
 * Copyright 2021 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.ibm.watson.modelmesh;

import java.util.concurrent.atomic.LongAdder;

/**
 * This class is currently only used for the instance-level request
 * rate or time tracking, not for each model copy within instances'
 * caches which works a bit differently.
 * @author nickhill
 */
public class RateTracker {

    private final long MULTIPLIER;

    // this is either request count or cumulative time in 1/100th millisecs
    private final LongAdder counter = new LongAdder();

    // these track the request count over time (each minute for last 30 mins)
    private volatile int lastIndex; // volatile TBD
    private final CountAtTime[] reqCounts = new CountAtTime[30 + 5];

    static class CountAtTime {
        final long timeMillis, count;

        public CountAtTime(long timeMillis, long count) {
            this.timeMillis = timeMillis;
            this.count = count;
        }
    }

    /**
     * @param timeTracking true for timeTracking mode, false for request
     *                     counting mode
     */
    public RateTracker(boolean timeTracking) {
        MULTIPLIER = timeTracking ? 10L : 60_000L;
    }

    Runnable reqCounterTask() {
        return () -> {
            long now = System.currentTimeMillis(), count = counter.sum();
            int nextIndex = lastIndex < reqCounts.length - 1 ? lastIndex + 1 : 0;
            reqCounts[nextIndex] = new CountAtTime(now, count);
            lastIndex = nextIndex;
        };
    }

    /*
     * This returns requests per minute when in rate-tracking mode,
     * or percentage processing time (in 10ths of a percent) when
     * in time-tracking mode.
     *
     * For example, if there were just two concurrent, very long
     * requests spanning the entire period, this would return a
     * value of 2000 when in time-tracking mode.
     *
     * NOTES related to rate-tracking mode:
     * This may not reflect discrete requests - batch requests can
     * count as more than one (additional items have reduced weight
     * to account for per-req overhead saving)
     *
     * TODO it would be better to make this a decay-weighted average
     * so the most recent interval has most weight and least recent
     * has least (but keep in mind that this method is currently
     * called quite frequently)
     *
     */
    public int getBusyness() {
        long now = System.currentTimeMillis(), count = counter.sum();
        // calculate rate in reqs per min over the last 6-7 minutes
        long time = now, cutoff = now - 360_000L;
        CountAtTime cat = null;
        for (int n = reqCounts.length, i = lastIndex + n; ; i--) {
            CountAtTime nextCat = reqCounts[i % n];
            if (nextCat == null || nextCat.timeMillis > time) {
                break;
            }
            cat = nextCat;
            if ((time = cat.timeMillis) <= cutoff) {
                break;
            }
        }
        return cat != null ? (int) ((count - cat.count) * MULTIPLIER / (now - cat.timeMillis)) : 0;
    }

    public long getTotalReqCount() {
        return counter.sum();
    }

    public void recordHits(int count) {
        counter.add(count);
    }

    /**
     * Record time taken in nanoseconds
     */
    public void recordTimeNanos(long nanos) {
        // convert to 1/100ths millis
        counter.add(nanos / 10_000L);
    }
}
