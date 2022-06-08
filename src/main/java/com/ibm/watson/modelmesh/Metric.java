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
    INVOKE_MODEL_COUNT("invoke-", "modelmesh_invoke_model", COUNTER_WITH_HISTO, //TODO _total suffix TBD
            "Count of internal model server inference requests"),
    INVOKE_MODEL_TIME("invoke-", "modelmesh_invoke_model_milliseconds", TIMING_HISTO,
            "Internal model server inference request time"),
    API_REQUEST_COUNT("api-", "modelmesh_api_request", COUNTER_WITH_HISTO, //TODO _total suffix TBD
            "Count of external inference requests"),
    API_REQUEST_TIME("api-", "modelmesh_api_request_milliseconds", TIMING_HISTO,
            "External inference request time"),

    // payload
    REQUEST_PAYLOAD_SIZE("reqSize-", "modelmesh_request_size_bytes", MESSAGE_SIZE_HISTO,
            "Inference request payload size"),
    RESPONSE_PAYLOAD_SIZE("respSize-", "modelmesh_response_size_bytes", MESSAGE_SIZE_HISTO,
            "Inference response payload size"),

    CACHE_MISS("cacheMiss", "modelmesh_cache_miss", COUNTER_WITH_HISTO,
            "Count of inference request cache misses"),
    CACHE_MISS_DELAY("cacheMiss", "modelmesh_cache_miss_milliseconds", TIMING_HISTO,
            "Cache miss delay"),
    LOAD_MODEL("loadModel", "modelmesh_loadmodel", COUNTER_WITH_HISTO,
            "Count of model loads"),
    LOAD_MODEL_TIME("loadModel", "modelmesh_loadmodel_milliseconds", TIMING_HISTO,
            "Time taken to load model"),
    UNLOAD_MODEL("unloadModel", "modelmesh_unloadmodel", COUNTER_WITH_HISTO,
            "Count of model unloads"),
    UNLOAD_MODEL_TIME("unloadModel", "modelmesh_unloadmodel_milliseconds", TIMING_HISTO,
            "Time taken to unload model"),

    // timing only (all millisecs)
    QUEUE_DELAY("queueDelay", "modelmesh_req_queue_delay_milliseconds", TIMING_HISTO,
            "Time spent in inference request queue"),
    LOAD_MODEL_QUEUE_DELAY("loadModelQueueDelay", "modelmesh_loading_queue_delay_milliseconds", TIMING_HISTO,
            "Time spent in model loading queue"),
    MODEL_SIZING_TIME("sizeModel", "modelmesh_model_sizing_milliseconds", TIMING_HISTO,
            "Time taken to perform model sizing"),

    // count only
    LOAD_MODEL_FAILURE("loadModel-failed", "modelmesh_loadmodel_failure", COUNTER,
            "Model load failures"),
    UNLOAD_MODEL_FAILURE("unloadModel-failure", "modelmesh_unloadmodel_failure", COUNTER,
            "Unload model failures (not counting multiple attempts for same copy)"),
    UNLOAD_MODEL_ATTEMPT_FAILURE("unloadModel-attemptFailed","modelmesh_unloadmodel_attempt_failure", COUNTER,
            "Unload model attempt failures"),

    // count plus age-at-eviction
    EVICT_MODEL("cache_evictModel", "modelmesh_model_evicted", COUNTER_WITH_HISTO,
            "Count of model copy evictions"),
    AGE_AT_EVICTION("cache_evictModel", "modelmesh_age_at_eviction_milliseconds", AGE_HISTO, // millisecs since last used
            "Time since model was last used when evicted"),

    // size (bytes)
    LOADED_MODEL_SIZE("mmModelSizeBytes", "modelmesh_loaded_model_size_bytes", MODEL_SIZE_HISTO,
            "Reported size of loaded model"),


    // ----- GAUGES

    // These three are global values for the logical modelmesh cluster
    MODELS_LOADED("modelsLoaded", "modelmesh_models_loaded_total", GAUGE, 1L, true,
            "Total number of models with at least one loaded copy"),
    MODELS_WITH_LOAD_FAIL("modelsWithLoadFailure", "modelmesh_models_with_failure_total", GAUGE, 1L, true,
            "Total number of models with one or more recent load failures"),
    TOTAL_MODELS("modelsManaged", "modelmesh_models_managed_total", GAUGE, 1L, true,
            "Total number of models managed"),

    // These are per instance values
    INSTANCE_LRU_TIME("localLruTimestamp", // seconds since epoch
            "modelmesh_instance_lru_seconds", GAUGE, "Last used time of least recently used model in pod (in secs since epoch)"),
    INSTANCE_LRU_AGE("localLruAge", // age in seconds
            "modelmesh_instance_lru_age_seconds", GAUGE, "Last used age of least recently used model in pod (secs ago)"),
    // Note the following two metrics are recorded in 8KiB units for
    // legacy statsd but bytes in new case
    INSTANCE_CAPACITY("totalCapacity", // in 8KiB units
            "modelmesh_instance_capacity_bytes", GAUGE, 8192L, false, "Effective model capacity of pod excluding unload buffer"),
    INSTANCE_USED("totalUsed", // in 8KiB units
            "modelmesh_instance_used_bytes", GAUGE, 8192L, false, "Amount of capacity currently in use by loaded models"),
    // Units are "basis points" (hundredths of a percent)
    INSTANCE_USED_BASIS_POINTS("capacityUsage", // derived from capacity & used values
            "modelmesh_instance_used_bps", GAUGE, "Amount of capacity used in basis points (100ths of percent)"),
    INSTANCE_MODEL_COUNT("modelCount",
            "modelmesh_instance_models_total", GAUGE, "Number of model copies loaded in pod");

    Metric(String oldStatsdName, String promName, MetricType type, long newMultiplier, boolean global, String description) {
        this.oldStatsdName = oldStatsdName;
        this.promName = promName;
        this.newStatsdName = promName + "_statsd";
        this.newMultiplier = newMultiplier;
        this.type = type;
        this.global = global;

        this.description = description;
    }

    Metric(String oldStatsdName, String newName, MetricType type, String description) {
        this(oldStatsdName, newName, type, 1L, false, description);
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
        MODEL_SIZE_HISTO,
        MESSAGE_SIZE_HISTO
    }
}
