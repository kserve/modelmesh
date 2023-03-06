package com.ibm.watson.modelmesh;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ModelMeshPerModelMetricsTest extends ModelMeshMetricsTest {
    @Override
    protected Map<String, String> extraEnvVars() {
        return ImmutableMap.of("MM_METRICS", "prometheus:port=" + METRICS_PORT + ";scheme=" + SCHEME +
                ";per_model_metrics=true");
    }
    @Override
    @Test
    public void verifyMetrics() throws Exception {
        // Insecure trust manager - skip TLS verification
        prepareMetrics();

        System.out.println(metrics.size() + " metrics scraped");

        // Spot check some expected metrics and values

        // External response time should all be < 2000ms (includes cache hit loading time)
        assertEquals(40.0,
                metrics.get("modelmesh_api_request_milliseconds_bucket{method=\"predict\",code=\"OK\",modelId=\"\"," +
                        "vModelId=\"\",le=\"2000.0\",}"));
        // External response time should all be < 200ms (includes cache hit loading time)
        assertEquals(40.0,
                metrics.get("modelmesh_invoke_model_milliseconds_bucket{method=\"predict\",code=\"OK\",modelId=\"\",vModelId=\"\",le=\"120000.0\",}"));
        // Simulated model sizing time is < 200ms
        assertEquals(1.0, metrics.get("modelmesh_model_sizing_milliseconds_bucket{modelId=\"myModel\",vModelId=\"\",le=\"60000.0\",}"));
        // Simulated model sizing time is > 50ms
        assertEquals(0.0, metrics.get("modelmesh_model_sizing_milliseconds_bucket{modelId=\"myModel\",vModelId=\"\",le=\"50.0\",}"));
        // Simulated model size is between 64MiB and 256MiB
        assertEquals(0.0, metrics.get("modelmesh_loaded_model_size_bytes_bucket{modelId=\"myModel\",vModelId=\"\",le=\"6.7108864E7\",}"));
        assertEquals(1.0, metrics.get("modelmesh_loaded_model_size_bytes_bucket{modelId=\"myModel\",vModelId=\"\",le=\"2.68435456E8\",}"));
        // One model is loaded
        assertEquals(1.0, metrics.get("modelmesh_instance_models_total"));
        // Histogram counts should reflect the two payload sizes (30 small, 10 large)
        assertEquals(30.0, metrics.get("modelmesh_request_size_bytes_bucket{method=\"predict\",code=\"OK\",modelId=\"\",vModelId=\"\",le=\"128.0\",}"));
        assertEquals(40.0, metrics.get("modelmesh_request_size_bytes_bucket{method=\"predict\",code=\"OK\",modelId=\"\",vModelId=\"\",le=\"2097152.0\",}"));
        assertEquals(30.0, metrics.get("modelmesh_response_size_bytes_bucket{method=\"predict\",code=\"OK\",modelId=\"\",vModelId=\"\",le=\"128.0\",}"));
        assertEquals(40.0, metrics.get("modelmesh_response_size_bytes_bucket{method=\"predict\",code=\"OK\",modelId=\"\",vModelId=\"\",le=\"2097152.0\",}"));

        // Memory metrics
        assertTrue(metrics.containsKey("netty_pool_mem_allocated_bytes{area=\"direct\",}"));
        assertTrue(metrics.containsKey("jvm_memory_pool_bytes_used{pool=\"G1 Eden Space\",}"));
        assertTrue(metrics.containsKey("jvm_buffer_pool_used_bytes{pool=\"direct\",}"));
        assertEquals(0.0, metrics.get("jvm_buffer_pool_used_buffers{pool=\"mapped\",}")); // mmapped memory not used
        assertTrue(metrics.containsKey("jvm_gc_collection_seconds_sum{gc=\"G1 Young Generation\",}"));
        assertTrue(metrics.containsKey("jvm_memory_bytes_committed{area=\"heap\",}"));
    }
}
