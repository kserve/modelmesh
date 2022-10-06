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
package com.ibm.watson.prometheus;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Exports netty pooled bytebuf allocation/usage metrics
 */
public final class NettyMemoryExports extends Collector {

    private static final List<String> AREA_LABEL = Collections.singletonList("area");
    private static final List<String> HEAP = Collections.singletonList("heap");
    private static final List<String> DIRECT = Collections.singletonList("direct");

    private final PooledByteBufAllocator allocator;

    public NettyMemoryExports() {
        this(PooledByteBufAllocator.DEFAULT);
    }

    NettyMemoryExports(PooledByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    public List<MetricFamilySamples> collect() {
        final GaugeMetricFamily allocMem = new GaugeMetricFamily(
                "netty_pool_mem_allocated_bytes",
                "Amount of memory allocated to netty buffer pools.",
                AREA_LABEL);
        final PooledByteBufAllocatorMetric metric = allocator.metric();
        allocMem.addMetric(HEAP, metric.usedHeapMemory());
        allocMem.addMetric(DIRECT, metric.usedDirectMemory());

        final GaugeMetricFamily usedMem = new GaugeMetricFamily(
                "netty_pool_mem_used_bytes",
                "Amount of netty pool memory allocated to app buffers.",
                AREA_LABEL);
        usedMem.addMetric(HEAP, allocator.pinnedHeapMemory());
        usedMem.addMetric(DIRECT, allocator.pinnedDirectMemory());

        return Arrays.asList(allocMem, usedMem);
    }
}
