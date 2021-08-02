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

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Encapsulates runtime parameters for the local instance
 */
public class LocalInstanceParameters {

    public static final int DEFAULT_LOADING_CONCURRENCY = 8;

    public static final long DEFAULT_LOAD_TIMEOUT_MS = 240_000L; // 4 mins for now

    // required - conservative estimate of "average" model size, in units
    private final int defaultModelSizeUnits;

    // size of threadpool used for loading models
    private int maxLoadingConcurrency = Integer.getInteger("tas.loading_thread_count", DEFAULT_LOADING_CONCURRENCY);

    private Set<String> acceptedModelTypes;

    private long loadTimeoutMs = DEFAULT_LOAD_TIMEOUT_MS;

    private boolean useHeap = true;

    // only applicable if useHeap == true, can be 0
    private long extraStaticHeapUsageMiB = Long.getLong("tas.static_heap_overhead_mbytes", 0L);

    // only applicable if useHeap == false
    private long capacityBytes;

    private boolean limitModelConcurrency;

    public LocalInstanceParameters(int defaultModelSizeUnits) {
        this.defaultModelSizeUnits = defaultModelSizeUnits;
    }

    public LocalInstanceParameters maxLoadingConcurrency(int maxLoadingConcurrency) {
        this.maxLoadingConcurrency = maxLoadingConcurrency;
        return this;
    }

    public LocalInstanceParameters useHeap() {
        useHeap = true;
        return this;
    }

    public LocalInstanceParameters useHeap(int staticUsageMiB) {
        useHeap = true;
        extraStaticHeapUsageMiB = staticUsageMiB;
        return this;
    }

    public LocalInstanceParameters useNonHeap(long capacityBytes) {
        useHeap = false;
        this.capacityBytes = capacityBytes;
        return this;
    }

    public LocalInstanceParameters acceptedModelTypes(String... typeStrings) {
        this.acceptedModelTypes = typeStrings == null ? null : ImmutableSet.copyOf(typeStrings);
        return this;
    }

    public LocalInstanceParameters modelLoadTimeoutMs(long loadTimeoutMs) {
        this.loadTimeoutMs = loadTimeoutMs;
        return this;
    }

    public LocalInstanceParameters limitModelConcurrency(boolean limit) {
        this.limitModelConcurrency = limit;
        return this;
    }

    public int getDefaultModelSizeUnits() {
        return defaultModelSizeUnits;
    }

    public int getMaxLoadingConcurrency() {
        return maxLoadingConcurrency;
    }

    public Set<String> getAcceptedModelTypes() {
        return acceptedModelTypes;
    }

    public boolean isUseHeap() {
        return useHeap;
    }

    public long getExtraStaticHeapUsageMiB() {
        return extraStaticHeapUsageMiB;
    }

    public long getCapacityBytes() {
        return capacityBytes;
    }

    public long getModelLoadTimeoutMs() {
        return loadTimeoutMs;
    }

    public boolean isLimitModelConcurrency() {
        return limitModelConcurrency;
    }
}
