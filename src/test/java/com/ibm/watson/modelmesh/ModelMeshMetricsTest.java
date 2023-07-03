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

import com.google.common.collect.ImmutableMap;
import com.ibm.watson.modelmesh.api.GetStatusRequest;
import com.ibm.watson.modelmesh.api.ModelInfo;
import com.ibm.watson.modelmesh.api.ModelMeshGrpc;
import com.ibm.watson.modelmesh.api.ModelMeshGrpc.ModelMeshBlockingStub;
import com.ibm.watson.modelmesh.api.ModelStatusInfo;
import com.ibm.watson.modelmesh.api.ModelStatusInfo.ModelStatus;
import com.ibm.watson.modelmesh.api.RegisterModelRequest;
import com.ibm.watson.modelmesh.api.UnregisterModelRequest;
import com.ibm.watson.modelmesh.example.api.ExamplePredictorGrpc;
import com.ibm.watson.modelmesh.example.api.ExamplePredictorGrpc.ExamplePredictorBlockingStub;
import com.ibm.watson.modelmesh.example.api.Predictor.PredictRequest;
import com.ibm.watson.modelmesh.example.api.Predictor.PredictResponse;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Model-mesh unit tests
 */
public class ModelMeshMetricsTest extends AbstractModelMeshClusterTest {

    @Override
    protected int replicaCount() {
        return 1;
    }

    protected int requestCount() {
        return 30;
    }

    static final int METRICS_PORT = 2112;

    static final String SCHEME = "https"; // or http

    static final String METRIC_NAME = "assistant_deployment_info:relabel";
    static final String DEPLOYMENT_NAME = "ga-tf-mm";
    static final String SLOT_NAME = "ga";
    static final String COMPONENT_NAME = "tf-mm";
    static final String GROUP_NAME = "clu";

    @Override
    protected Map<String, String> extraEnvVars() {
        return ImmutableMap.of("MM_METRICS", "prometheus:port=" + METRICS_PORT + ";scheme=" + SCHEME +
                ";per_model_metrics=true");
    }

    @BeforeAll
    public void metricsTest() throws Exception {

        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 9000).usePlaintext().build();
        try {
            ModelMeshBlockingStub manageModels = ModelMeshGrpc.newBlockingStub(channel);

            ExamplePredictorBlockingStub useModels = ExamplePredictorGrpc.newBlockingStub(channel);

            // verify not found status
            ModelStatusInfo status = manageModels.getModelStatus(GetStatusRequest.newBuilder()
                    .setModelId("I don't exist").build());

            assertEquals(ModelStatus.NOT_FOUND, status.getStatus());
            assertEquals(0, status.getErrorsCount());

            // add a model
            String modelId = "myModel";
            ModelStatusInfo statusInfo = manageModels.registerModel(RegisterModelRequest.newBuilder()
                    .setModelId(modelId).setModelInfo(ModelInfo.newBuilder().setType("ExampleType").build())
                    .setLoadNow(true).build());

            System.out.println("registerModel returned: " + statusInfo.getStatus());

            // call predict a bunch of times on the model
            PredictRequest req = PredictRequest.newBuilder().setText("predict me!").build();

            System.out.println("Calling predict many times with small payload");
            long before = System.nanoTime();
            for (int i = 0; i < requestCount(); i++) {
                PredictResponse response = forModel(useModels, modelId).predict(req);
                assertEquals(1.0, response.getResults(0).getConfidence(), 0);
                assertEquals("classification for predict me! by model myModel",
                        response.getResults(0).getCategory());
            }
            System.out.println("Took " + (System.nanoTime() - before) / 1000_000_000L + "sec");

            // verify getStatus
            status = manageModels.getModelStatus(GetStatusRequest.newBuilder()
                    .setModelId(modelId).build());

            assertEquals(ModelStatus.LOADED, status.getStatus());
            assertEquals(0, status.getErrorsCount());

            System.out.println("Calling predict 10 times with larger payload");
            // verify larger payload
            int bigChars = 2_000_000;
            StringBuilder sb = new StringBuilder(bigChars);
            for (int i = 0; i < bigChars; i++) {
                sb.append('a');
            }
            String toSend = sb.toString();
            req = PredictRequest.newBuilder().setText(toSend).build();

            for (int i = 0; i < 10; i++) {
                PredictResponse response = forModel(useModels, modelId)
                        .withDeadlineAfter(2, TimeUnit.SECONDS).predict(req);
                assertEquals("classification for " + toSend + " by model myModel",
                        response.getResults(0).getCategory());
            }

            // grab and verify prometheus metrics
            verifyMetrics();

            // delete
            manageModels.unregisterModel(UnregisterModelRequest.newBuilder()
                    .setModelId(modelId).build());
        } finally {
            channel.shutdown();
        }
    }

    protected Map<String,Double> prepareMetrics() throws Exception {
        // Insecure trust manager - skip TLS verification
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, InsecureTrustManagerFactory.INSTANCE.getTrustManagers(), null);
        
        HttpClient client = HttpClient.newBuilder().sslContext(sslContext).build();
        HttpRequest metricsRequest = HttpRequest.newBuilder()
                .uri(URI.create(SCHEME + "://localhost:" + METRICS_PORT + "/metrics")).build();

        HttpResponse<Stream<String>> resp = client.send(metricsRequest, HttpResponse.BodyHandlers.ofLines());

        assertEquals(200, resp.statusCode());

        //resp.body().forEach(System.out::println);

        final Pattern line = Pattern.compile("([^\\s{]+(?:\\{.+\\})?)\\s+(\\S+)");

        Map<String,Double> metrics = resp.body().filter(s -> !s.startsWith("#")).map(s -> line.matcher(s))
                .filter(Matcher::matches)
                .collect(Collectors.toMap(m -> m.group(1), m -> Double.parseDouble(m.group(2))));

        return metrics;
    }

    @Test
    public void verifyMetrics() throws Exception {
        // Insecure trust manager - skip TLS verification
        Map<String,Double> metrics = prepareMetrics();

        System.out.println(metrics.size() + " metrics scraped");

        // Spot check some expected metrics and values

        // External response time should all be < 2000ms (includes cache hit loading time)
        assertEquals(40.0, metrics.get("modelmesh_api_request_milliseconds_bucket{method=\"predict\",code=\"OK\",modelId=\"myModel\",vModelId=\"\",le=\"2000.0\",}"));
        // External response time should all be < 200ms (includes cache hit loading time)
        assertEquals(40.0,
                metrics.get("modelmesh_invoke_model_milliseconds_bucket{method=\"predict\",code=\"OK\",modelId=\"myModel\",vModelId=\"\",le=\"120000.0\",}"));
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
        assertEquals(30.0, metrics.get("modelmesh_request_size_bytes_bucket{method=\"predict\",code=\"OK\",modelId=\"myModel\",vModelId=\"\",le=\"128.0\",}"));
        assertEquals(40.0, metrics.get("modelmesh_request_size_bytes_bucket{method=\"predict\",code=\"OK\",modelId=\"myModel\",vModelId=\"\",le=\"2097152.0\",}"));

        // Memory metrics
        assertTrue(metrics.containsKey("netty_pool_mem_allocated_bytes{area=\"direct\",}"));
        assertTrue(metrics.containsKey("jvm_memory_pool_bytes_used{pool=\"G1 Eden Space\",}"));
        assertTrue(metrics.containsKey("jvm_buffer_pool_used_bytes{pool=\"direct\",}"));
        assertEquals(0.0, metrics.get("jvm_buffer_pool_used_buffers{pool=\"mapped\",}")); // mmapped memory not used
        assertTrue(metrics.containsKey("jvm_gc_collection_seconds_sum{gc=\"G1 Young Generation\",}"));
        assertTrue(metrics.containsKey("jvm_memory_bytes_committed{area=\"heap\",}"));

        // Info metrics
        assertEquals(1.0, metrics.get(METRIC_NAME + "{component=\"" + COMPONENT_NAME
                + "\",slot=\"" + SLOT_NAME + "\",deployment=\"" + DEPLOYMENT_NAME + "\",group=\"" + GROUP_NAME + "\",}"));
    }

}