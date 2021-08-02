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

/**
 * Used to track mean and variance of a stream of times. Uses locking
 * for updates, so shouldn't be updated by super high frequency streams
 * (i.e. not ideal to use for invocation response times, but fine for
 * model loading times).
 */
public class TimeStats {
    private volatile long count;
    private volatile double mean;
    private double deltaSqSum;

    private int assumeCompleteAfterMillis;

    public TimeStats(long defaultTimeMillis) {
        assumeCompleteAfterMillis = Math.toIntExact(defaultTimeMillis);
    }

    public int assumeCompletedAfterMillis() {
        return assumeCompleteAfterMillis;
    }

    public long count() {
        return count;
    }

    public long mean() {
        return (long) mean;
    }

    private static final int MASK = 4 - 1; // sample one in every 4

    public void recordTime(long timeMillis) {
        long newCount;
        double t = timeMillis, newSum, newMean;
        synchronized (this) {
            newCount = ++count;
            double delta = t - mean;
            newMean = mean += delta / newCount;
            newSum = deltaSqSum += delta * (t - newMean);
        }
        if ((newCount & MASK) == 0) {
            // use default if < 4 samples, more conservative if a small number of samples
            double nsd = newCount <= 20 ? 5.0 : 3.0;
            assumeCompleteAfterMillis = (int) (newMean + nsd * Math.sqrt(newSum / newCount));
        } else if (newCount < 4) {
            // Shift default using measurements so far, in case it is set very wrong
            int current = assumeCompleteAfterMillis;
            assumeCompleteAfterMillis = (int) ((current + current + newMean) / 3.0);
        }
    }
}
