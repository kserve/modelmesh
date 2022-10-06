/*
 * Copyright 2022 IBM Corporation
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

/*
 * This is a modified (optimized) version of the equivalent class from the official
 * Apache 2.0 licensed prometheus java client https://github.com/prometheus/client_java,
 * which is also a dependency.
 *
 * The current mods are based on version 0.9.0
 */
package com.ibm.watson.prometheus.hotspot;

import com.ibm.watson.prometheus.Counter;
import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import io.prometheus.client.Collector;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class MemoryAllocationExports extends Collector {
    private final Counter allocatedCounter = Counter.build()
            .name("jvm_memory_pool_allocated_bytes_total")
            .help("Total bytes allocated in a given JVM memory pool. Only updated after GC, not continuously.")
            .labelNames("pool")
            .create();

    public MemoryAllocationExports() {
        AllocationCountingNotificationListener listener = new AllocationCountingNotificationListener(allocatedCounter);
        for (GarbageCollectorMXBean garbageCollectorMXBean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (garbageCollectorMXBean instanceof NotificationEmitter) {
                ((NotificationEmitter) garbageCollectorMXBean).addNotificationListener(listener, null, null);
            }
        }
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return allocatedCounter.collect();
    }

    static final class MemUsage {
        long usage;

        long getAndSet(long value) {
            long before = usage;
            usage = value;
            return before;
        }
    }

    static final class AllocationCountingNotificationListener implements NotificationListener {
        private final Map<String, MemUsage> lastMemoryUsage = new HashMap<>(8);
        private final Counter counter;

        AllocationCountingNotificationListener(Counter counter) {
            this.counter = counter;
        }

        @Override
        public synchronized void handleNotification(Notification notification, Object handback) {
            final GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
            final GcInfo gcInfo = info.getGcInfo();
            Map<String, MemoryUsage> memoryUsageBeforeGc = gcInfo.getMemoryUsageBeforeGc();
            Map<String, MemoryUsage> memoryUsageAfterGc = gcInfo.getMemoryUsageAfterGc();
            for (Map.Entry<String, MemoryUsage> entry : memoryUsageBeforeGc.entrySet()) {
                final String memoryPool = entry.getKey();
                final long before = entry.getValue().getUsed();
                final long after = memoryUsageAfterGc.get(memoryPool).getUsed();
                handleMemoryPool(memoryPool, before, after);
            }
        }

        // Visible for testing
        void handleMemoryPool(String memoryPool, long before, long after) {
            /*
             * Calculate increase in the memory pool by comparing memory used
             * after last GC, before this GC, and after this GC.
             * See ascii illustration below.
             * Make sure to count only increases and ignore decreases.
             * (Typically a pool will only increase between GCs or during GCs, not both.
             * E.g. eden pools between GCs. Survivor and old generation pools during GCs.)
             *
             *                         |<-- diff1 -->|<-- diff2 -->|
             * Timeline: |-- last GC --|             |---- GC -----|
             *                      ___^__        ___^____      ___^___
             * Mem. usage vars:    / last \      / before \    / after \
             */

            // Get last memory usage after GC and remember memory used after for next time
            long last = getAndSet(lastMemoryUsage, memoryPool, after);
            // Difference since last GC
            long diff1 = before - last;
            // Difference during this GC
            long diff2 = after - before;
            // Make sure to only count increases
            if (diff1 < 0) {
                diff1 = 0;
            }
            if (diff2 < 0) {
                diff2 = 0;
            }
            long increase = diff1 + diff2;
            if (increase > 0) {
                counter.labels(memoryPool).inc(increase);
            }
        }

        private static long getAndSet(Map<String, MemUsage> map, String key, long value) {
            return map.computeIfAbsent(key, k -> new MemUsage()).getAndSet(value);
        }
    }
}
