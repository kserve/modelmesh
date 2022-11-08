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

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

/**
 * Exports metrics about JVM buffers.
 *
 */
public final class BufferPoolsExports extends Collector {

    private static final List<String> POOL_LABEL = Collections.singletonList("pool");

    private final BufferPoolMXBean[] bufferPoolMXBeans;
    private final List<String>[] poolBeanNames;

    public BufferPoolsExports() {
        final List<BufferPoolMXBean> poolBeans = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        this.bufferPoolMXBeans = poolBeans.toArray(BufferPoolMXBean[]::new);
        this.poolBeanNames = poolBeans.stream().map(pb ->
                Collections.singletonList(pb.getName())).toArray(List[]::new);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        GaugeMetricFamily used = new GaugeMetricFamily(
                "jvm_buffer_pool_used_bytes",
                "Used bytes of a given JVM buffer pool.",
                POOL_LABEL);
        GaugeMetricFamily capacity = new GaugeMetricFamily(
                "jvm_buffer_pool_capacity_bytes",
                "Bytes capacity of a given JVM buffer pool.",
                POOL_LABEL);
        GaugeMetricFamily buffers = new GaugeMetricFamily(
                "jvm_buffer_pool_used_buffers",
                "Used buffers of a given JVM buffer pool.",
                POOL_LABEL);
        for (int i = 0; i < bufferPoolMXBeans.length; i++) {
            final List<String> poolName = poolBeanNames[i];
            final BufferPoolMXBean pool = bufferPoolMXBeans[i];
            used.addMetric(poolName, pool.getMemoryUsed());
            capacity.addMetric(poolName, pool.getTotalCapacity());
            buffers.addMetric(poolName, pool.getCount());
        }
        return Arrays.asList(used, capacity, buffers);
    }
}
