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
import io.prometheus.client.GaugeMetricFamily;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Exports metrics about JVM memory areas.
 * <p>
 * Example usage:
 * <pre>
 * {@code
 *   new MemoryPoolsExports().register();
 * }
 * </pre>
 * Example metrics being exported:
 * <pre>
 *   jvm_memory_bytes_used{area="heap"} 2000000
 *   jvm_memory_bytes_committed{area="nonheap"} 200000
 *   jvm_memory_bytes_max{area="nonheap"} 2000000
 *   jvm_memory_pool_bytes_used{pool="PS Eden Space"} 2000
 * </pre>
 */
public final class MemoryPoolsExports extends Collector {

    private static final List<String> AREA_LABEL = Collections.singletonList("area");
    private static final List<String> POOL_LABEL = Collections.singletonList("pool");
    private static final List<String> HEAP = Collections.singletonList("heap");
    private static final List<String> NONHEAP = Collections.singletonList("nonheap");;

    private final MemoryMXBean memoryBean;
    private final MemoryPoolMXBean[] poolBeans;
    private final List<String>[] poolBeanNames;

    public MemoryPoolsExports() {
        this(
                ManagementFactory.getMemoryMXBean(),
                ManagementFactory.getMemoryPoolMXBeans());
    }

    public MemoryPoolsExports(MemoryMXBean memoryBean,
                              List<MemoryPoolMXBean> poolBeans) {
        this.memoryBean = memoryBean;
        this.poolBeans = poolBeans.toArray(MemoryPoolMXBean[]::new);
        this.poolBeanNames = poolBeans.stream().map(pb ->
                Collections.singletonList(pb.getName())).toArray(List[]::new);
    }

    void addMemoryAreaMetrics(List<MetricFamilySamples> sampleFamilies) {
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        MemoryUsage nonHeapUsage = memoryBean.getNonHeapMemoryUsage();

        GaugeMetricFamily used = new GaugeMetricFamily(
                "jvm_memory_bytes_used",
                "Used bytes of a given JVM memory area.",
                AREA_LABEL);
        used.addMetric(HEAP, heapUsage.getUsed());
        used.addMetric(NONHEAP, nonHeapUsage.getUsed());
        sampleFamilies.add(used);

        GaugeMetricFamily committed = new GaugeMetricFamily(
                "jvm_memory_bytes_committed",
                "Committed (bytes) of a given JVM memory area.",
                AREA_LABEL);
        committed.addMetric(Collections.singletonList("heap"), heapUsage.getCommitted());
        committed.addMetric(NONHEAP, nonHeapUsage.getCommitted());
        sampleFamilies.add(committed);

        GaugeMetricFamily max = new GaugeMetricFamily(
                "jvm_memory_bytes_max",
                "Max (bytes) of a given JVM memory area.",
                AREA_LABEL);
        max.addMetric(Collections.singletonList("heap"), heapUsage.getMax());
        max.addMetric(NONHEAP, nonHeapUsage.getMax());
        sampleFamilies.add(max);

        GaugeMetricFamily init = new GaugeMetricFamily(
                "jvm_memory_bytes_init",
                "Initial bytes of a given JVM memory area.",
                AREA_LABEL);
        init.addMetric(Collections.singletonList("heap"), heapUsage.getInit());
        init.addMetric(NONHEAP, nonHeapUsage.getInit());
        sampleFamilies.add(init);
    }

    void addMemoryPoolMetrics(List<MetricFamilySamples> sampleFamilies) {
        GaugeMetricFamily used = new GaugeMetricFamily(
                "jvm_memory_pool_bytes_used",
                "Used bytes of a given JVM memory pool.",
                POOL_LABEL);
        sampleFamilies.add(used);
        GaugeMetricFamily committed = new GaugeMetricFamily(
                "jvm_memory_pool_bytes_committed",
                "Committed bytes of a given JVM memory pool.",
                POOL_LABEL);
        sampleFamilies.add(committed);
        GaugeMetricFamily max = new GaugeMetricFamily(
                "jvm_memory_pool_bytes_max",
                "Max bytes of a given JVM memory pool.",
                POOL_LABEL);
        sampleFamilies.add(max);
        GaugeMetricFamily init = new GaugeMetricFamily(
                "jvm_memory_pool_bytes_init",
                "Initial bytes of a given JVM memory pool.",
                POOL_LABEL);
        sampleFamilies.add(init);
        for (int i = 0; i < poolBeans.length ; i++) {
            final List<String> poolName = poolBeanNames[i];
            final MemoryUsage poolUsage = poolBeans[i].getUsage();
            used.addMetric(poolName, poolUsage.getUsed());
            committed.addMetric(poolName, poolUsage.getCommitted());
            max.addMetric(poolName, poolUsage.getMax());
            init.addMetric(poolName, poolUsage.getInit());
        }
    }

    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<MetricFamilySamples>(8);
        addMemoryAreaMetrics(mfs);
        addMemoryPoolMetrics(mfs);
        return mfs;
    }
}
