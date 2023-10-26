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

import com.google.common.util.concurrent.Service;
import com.ibm.watson.kvutils.factory.KVUtilsFactory;
import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import com.ibm.watson.litelinks.server.LitelinksService;
import com.ibm.watson.litelinks.server.LitelinksService.ServiceDeploymentConfig;
import com.ibm.watson.modelmesh.example.ExampleModelRuntime;
import com.ibm.watson.modelmesh.example.api.ExamplePredictorGrpc;
import com.ibm.watson.modelmesh.example.api.ExamplePredictorGrpc.ExamplePredictorBlockingStub;
import com.ibm.watson.modelmesh.example.api.Predictor.MultiPredictResponse;
import com.ibm.watson.modelmesh.example.api.Predictor.PredictRequest;
import com.ibm.watson.modelmesh.example.api.Predictor.PredictResponse;
import com.ibm.watson.tas.proto.TasRuntimeGrpc;
import com.ibm.watson.tas.proto.TasRuntimeGrpc.TasRuntimeBlockingStub;
import com.ibm.watson.tas.proto.TasRuntimeOuterClass.AddModelRequest;
import com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelRequest;
import com.ibm.watson.tas.proto.TasRuntimeOuterClass.GetStatusRequest;
import com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelInfo;
import com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo;
import com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo.ModelStatus;
import com.ibm.watson.zk.ZookeeperClient;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Train-and-serve runtime service unit tests
 */
public class LegacyTasProtoTest {

    // Shared infrastructure
    private static TestingServer localZk;
    private static String localZkConnStr;

    private static final String tasRuntimeStandaloneName = "tas-runtime-sidecar-test";

    static Service tasService;
    static ExampleModelRuntime exampleRuntime;

    @BeforeAll
    public static void initialize() throws Exception {
        //shared infrastructure
        setupZookeeper();

        // Create standalone TAS service
        System.setProperty(ModelMeshEnvVars.GRPC_PORT_ENV_VAR, "8088");
        tasService = LitelinksService.createService(
                new ServiceDeploymentConfig(SidecarModelMesh.class)
                        .setServiceRegistry("zookeeper:" + localZkConnStr)
                        .setServiceName(tasRuntimeStandaloneName)
                        .setServiceVersion("20170315-1347"));

        System.out.println("starting tas server");
        tasService.startAsync();

        System.out.println("starting example runtime server");
        exampleRuntime = new ExampleModelRuntime(8085).start();

        System.out.println("waiting for tas start");
        tasService.awaitRunning();
        System.out.println("tas start complete");
        System.clearProperty(ModelMeshEnvVars.GRPC_PORT_ENV_VAR);
    }

    @Test
    public void grpcTest() throws Exception {

        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 8088)
                .usePlaintext().build();
        try {
            TasRuntimeBlockingStub manageModels = TasRuntimeGrpc.newBlockingStub(channel);

            ExamplePredictorBlockingStub useModels = ExamplePredictorGrpc.newBlockingStub(channel);

            // add a model
            String modelId = "myModel";
            ModelStatusInfo statusInfo = manageModels.addModel(AddModelRequest.newBuilder()
                    .setModelId(modelId).setModelInfo(ModelInfo.newBuilder().setType("ExampleType").build())
                    .setLoadNow(true).build());

            System.out.println("addModel returned: " + statusInfo.getStatus());

            // call predict on the model
            PredictRequest req = PredictRequest.newBuilder().setText("predict me!").build();
            PredictResponse response = forModel(useModels, modelId)
                    .predict(req);

            System.out.println("predict returned: " + response.getResultsList());

            assertEquals(1.0, response.getResults(0).getConfidence(), 0);

            assertEquals("classification for predict me! by model myModel",
                    response.getResults(0).getCategory());

            // verify getStatus
            ModelStatusInfo status = manageModels.getModelStatus(GetStatusRequest.newBuilder()
                    .setModelId(modelId).build());

            assertEquals(ModelStatus.LOADED, status.getStatus());
            assertEquals(0, status.getErrorsCount());

            // send special command to purge model cache
            forModel(useModels, modelId).predict(PredictRequest
                    .newBuilder().setText("test:purge-models").build());

            // subsequent predict call should succeed
            req = PredictRequest.newBuilder().setText("predict me again!").build();
            response = forModel(useModels, modelId).predict(req);
            assertEquals("classification for predict me again! by model myModel",
                    response.getResults(0).getCategory());

            // verify larger payload
            int bigChars = 2_000_000;
            StringBuilder sb = new StringBuilder(bigChars);
            for (int i = 0; i < bigChars; i++) {
                sb.append('a');
            }
            String toSend = sb.toString();
            req = PredictRequest.newBuilder().setText(toSend).build();
            response = forModel(useModels, modelId).withDeadlineAfter(2, TimeUnit.SECONDS)
                    .predict(req);
            assertEquals("classification for " + toSend + " by model myModel",
                    response.getResults(0).getCategory());

            // delete
            manageModels.deleteModel(DeleteModelRequest.newBuilder()
                    .setModelId(modelId).build());
        } finally {
            channel.shutdown();
        }
    }

    @Test
    public void multiModelTest() throws Exception {

        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 8088)
                .usePlaintext().build();
        try {
            TasRuntimeBlockingStub manageModels = TasRuntimeGrpc.newBlockingStub(channel);

            ExamplePredictorBlockingStub useModels = ExamplePredictorGrpc.newBlockingStub(channel);

            // add 3 models
            for (int i = 1; i <= 3; i++) {
                String modelId = "myModel-" + i;
                ModelStatusInfo statusInfo = manageModels.addModel(AddModelRequest.newBuilder()
                        .setModelId(modelId).setModelInfo(ModelInfo.newBuilder().setType("ExampleType").build())
                        .setLoadNow(true).build());
                System.out.println("addModel " + i + " returned: " + statusInfo.getStatus());
            }

            // call predict on the model
            PredictRequest req = PredictRequest.newBuilder().setText("sometext").build();
            MultiPredictResponse response = forModel(useModels,
                    "myModel-1", "myModel-2", "myModel-3").multiPredict(req);

            Map<String, PredictResponse> map = response.getPerModelResultsMap();
            System.out.println("predict returned: " + map);

            assertEquals(3, map.size());

            for (int i = 1; i <= 3; i++) {
                PredictResponse resp = map.get("myModel-" + i);
                assertNotNull(resp);
                assertEquals(2, resp.getResultsCount());
                assertEquals("classification for sometext by model myModel-" + i,
                        resp.getResults(0).getCategory());

                manageModels.deleteModel(DeleteModelRequest.newBuilder()
                        .setModelId("myModel-i").build());
            }

            //TODO also test multi-vmodel

        } finally {
            channel.shutdown();
        }
    }

    static final Metadata.Key<String> MODEL_ID_META_KEY =
            Metadata.Key.of("mm-model-id", Metadata.ASCII_STRING_MARSHALLER);

    static final Metadata.Key<String> CUST_HEADER_KEY =
            Metadata.Key.of("another-custom-header", Metadata.ASCII_STRING_MARSHALLER);

    public static <T extends AbstractStub<T>> T forModel(T stub, String... modelIds) {
        Metadata headers = new Metadata();
        for (String modelId : modelIds) {
            headers.put(MODEL_ID_META_KEY, modelId);
        }
        headers.put(CUST_HEADER_KEY, "custom-value");
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    @BeforeEach
    public void beforeEachTest() {
        System.out.println("[Client] ------------------   Starting New Test -------------------");
    }

    @AfterEach
    public void afterEachTest() {
        System.out.println("[Client] ------------------   Finished Test -------------------");
    }

    @AfterAll
    public static void shutdown() throws IOException, InterruptedException {
        System.out.println("stopping tas server");
        tasService.stopAsync().awaitTerminated();

        System.out.println("tas server stop complete, stopping runtime server");
        exampleRuntime.shutdown().awaitTermination();
        System.out.println("runtime server stop complete");

        ZookeeperClient.shutdown(false, false);
        KVUtilsFactory.resetDefaultFactory();
        if (localZk != null) {
            localZk.close();
        }
    }


    private static void setupZookeeper() throws Exception {
        // Local ZK setup
        localZk = new TestingServer();
        localZk.start();
        localZkConnStr = localZk.getConnectString();
        System.setProperty(LitelinksSystemPropNames.SERVER_REGISTRY,
                "zookeeper:" + localZkConnStr);
        System.setProperty(KVUtilsFactory.KV_STORE_EV,
                "zookeeper:" + localZkConnStr);
    }

}
