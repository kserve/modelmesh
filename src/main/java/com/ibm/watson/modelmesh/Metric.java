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

import static com.ibm.watson.modelmesh.Metric.MetricType.*;

/**
 *
 */
enum Metric {

    // ------ EVENTS

    // count plus timing
    INVOKE_MODEL_COUNT("invoke-", "modelmesh_invoke_model", COUNTER_WITH_HISTO), //TODO _total suffix TBD
    INVOKE_MODEL_TIME("invoke-", "modelmesh_invoke_model_milliseconds", TIMING_HISTO),
    API_REQUEST_COUNT("api-", "modelmesh_api_request", COUNTER_WITH_HISTO), //TODO _total suffix TBD
    API_REQUEST_TIME("api-", "modelmesh_api_request_milliseconds", TIMING_HISTO),

    CACHE_MISS("cacheMiss", "modelmesh_cache_miss", COUNTER_WITH_HISTO),
    CACHE_MISS_DELAY("cacheMiss", "modelmesh_cache_miss_milliseconds", TIMING_HISTO),
    LOAD_MODEL("loadModel", "modelmesh_loadmodel", COUNTER_WITH_HISTO),
    LOAD_MODEL_TIME("loadModel", "modelmesh_loadmodel_milliseconds", TIMING_HISTO),
    UNLOAD_MODEL("unloadModel", "modelmesh_unloadmodel", COUNTER_WITH_HISTO),
    UNLOAD_MODEL_TIME("unloadModel", "modelmesh_unloadmodel_milliseconds", TIMING_HISTO),

    // timing only (all millisecs)
    QUEUE_DELAY("queueDelay",
            "modelmesh_req_queue_delay_milliseconds", TIMING_HISTO),
    LOAD_MODEL_QUEUE_DELAY("loadModelQueueDelay",
            "modelmesh_loading_queue_delay_milliseconds", TIMING_HISTO),
    MODEL_SIZING_TIME("sizeModel",
            "modelmesh_model_sizing_milliseconds", TIMING_HISTO),

    // count only
    LOAD_MODEL_FAILURE("loadModel-failed",
            "modelmesh_loadmodel_failure", COUNTER),
    UNLOAD_MODEL_FAILURE("unloadModel-failure",
            "modelmesh_unloadmodel_failure", COUNTER),
    UNLOAD_MODEL_ATTEMPT_FAILURE("unloadModel-attemptFailed",
            "modelmesh_unloadmodel_attempt_failure", COUNTER),

    // count plus age-at-eviction
    EVICT_MODEL("cache_evictModel",
            "modelmesh_model_evicted", COUNTER_WITH_HISTO),
    AGE_AT_EVICTION("cache_evictModel", // millisecs since last used
            "modelmesh_age_at_eviction_milliseconds", AGE_HISTO),

    // size (bytes)
    LOADED_MODEL_SIZE("mmModelSizeBytes",
            "modelmesh_loaded_model_size_bytes", SIZE_HISTO),


    // ----- GAUGES

    // These three are global values for the logical modelmesh cluster
    MODELS_LOADED("modelsLoaded",
            "modelmesh_models_loaded_total", GAUGE, 1L, true),
    MODELS_WITH_LOAD_FAIL("modelsWithLoadFailure",
            "modelmesh_models_with_failure_total", GAUGE, 1L, true),
    TOTAL_MODELS("modelsManaged",
            "modelmesh_models_managed_total", GAUGE, 1L, true),

    // These are per instance values
    INSTANCE_LRU_TIME("localLruTimestamp", // seconds since epoch
            "modelmesh_instance_lru_seconds", GAUGE),
    INSTANCE_LRU_AGE("localLruAge", // age in seconds
            "modelmesh_instance_lru_age_seconds", GAUGE),
    // Note the following two metrics are recorded in 8KiB units for
    // legacy statsd but bytes in new case
    INSTANCE_CAPACITY("totalCapacity", // in 8KiB units
            "modelmesh_instance_capacity_bytes", GAUGE, 8192L, false),
    INSTANCE_USED("totalUsed", // in 8KiB units
            "modelmesh_instance_used_bytes", GAUGE, 8192L, false),
    // Units are "basis points" (hundredths of a percent)
    INSTANCE_USED_BASIS_POINTS("capacityUsage", // derived from capacity & used values
            "modelmesh_instance_used_bps", GAUGE),
    INSTANCE_MODEL_COUNT("modelCount",
            "modelmesh_instance_models_total", GAUGE);

    Metric(String oldStatsdName, String promName, MetricType type, long newMultiplier, boolean global) {
        this.oldStatsdName = oldStatsdName;
        this.promName = promName;
        this.newStatsdName = promName + "_statsd";
        this.newMultiplier = newMultiplier;
        this.type = type;
        this.global = global;

        this.description = promName; //TODO include descriptions
    }

    Metric(String oldStatsdName, String newName, MetricType type) {
        this(oldStatsdName, newName, type, 1L, false);
    }

    final String newStatsdName;
    final String oldStatsdName;
    final String promName;

    final String description;

    final long newMultiplier;

    final boolean global;

    final MetricType type;

    public enum MetricType {
        COUNTER,
        // used for counters which have corresponding timing histogram
        // - these excluded for prometheus since histogram also records count
        COUNTER_WITH_HISTO,
        GAUGE,
        TIMING_HISTO,
        AGE_HISTO,
        SIZE_HISTO
    }
}
