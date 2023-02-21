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

import io.prometheus.client.Collector;
import io.prometheus.client.SummaryMetricFamily;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Exports metrics about JVM garbage collectors.
 * <p>
 * Example usage:
 * <pre>
 * {@code
 *   new GarbageCollectorExports().register();
 * }
 * </pre>
 * Example metrics being exported:
 * <pre>
 *   jvm_gc_collection_seconds_count{gc="PS1"} 200
 *   jvm_gc_collection_seconds_sum{gc="PS1"} 6.7
 * </pre>
 */
public final class GarbageCollectorExports extends Collector {

    private static final List<String> GC_LABEL = Collections.singletonList("gc");

    private final GarbageCollectorMXBean[] garbageCollectors;
    private final List<String>[] garbageCollectorNames;

    public GarbageCollectorExports() {
        this(ManagementFactory.getGarbageCollectorMXBeans());
    }

    GarbageCollectorExports(List<GarbageCollectorMXBean> garbageCollectors) {
        this.garbageCollectors = garbageCollectors.toArray(GarbageCollectorMXBean[]::new);
        this.garbageCollectorNames = garbageCollectors.stream().map(gc ->
                Collections.singletonList(gc.getName())).toArray(List[]::new);
    }

    public List<MetricFamilySamples> collect() {
        SummaryMetricFamily gcCollection = new SummaryMetricFamily(
                "jvm_gc_collection_seconds",
                "Time spent in a given JVM garbage collector in seconds.",
                GC_LABEL);
        for (int i = 0; i < garbageCollectors.length; i++) {
            final GarbageCollectorMXBean gc = garbageCollectors[i];
            gcCollection.addMetric(garbageCollectorNames[i],
                    gc.getCollectionCount(),
                    gc.getCollectionTime() / MILLISECONDS_PER_SECOND);
        }
        return Collections.singletonList(gcCollection);
    }
}
