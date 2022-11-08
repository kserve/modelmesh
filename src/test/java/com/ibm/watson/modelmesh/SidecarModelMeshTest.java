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

import com.google.common.base.Strings;
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
import com.ibm.watson.modelmesh.example.api.Predictor.MultiPredictResponse;
import com.ibm.watson.modelmesh.example.api.Predictor.PredictRequest;
import com.ibm.watson.modelmesh.example.api.Predictor.PredictResponse;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Model-mesh basic roundtrip tests
 */
public class SidecarModelMeshTest extends SingleInstanceModelMeshTest {

    @Test
    public void grpcTest() throws Exception {

        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 8088)
                .usePlaintext().build();
        try {
            ModelMeshBlockingStub manageModels = ModelMeshGrpc.newBlockingStub(channel);

            ExamplePredictorBlockingStub useModels = ExamplePredictorGrpc.newBlockingStub(channel);

            // verify not found status
            ModelStatusInfo status = manageModels.getModelStatus(GetStatusRequest.newBuilder()
                    .setModelId("i don't exist").build());

            assertEquals(ModelStatus.NOT_FOUND, status.getStatus());
            assertEquals(0, status.getErrorsCount());


            // add a model
            String modelId = "myModel";
            ModelStatusInfo statusInfo = manageModels.registerModel(RegisterModelRequest.newBuilder()
                    .setModelId(modelId).setModelInfo(ModelInfo.newBuilder().setType("ExampleType").build())
                    .setLoadNow(true).build());

            System.out.println("registerModel returned: " + statusInfo);

            // call predict on the model
            PredictRequest req = PredictRequest.newBuilder().setText("predict me!").build();
            PredictResponse response = forModel(useModels, modelId)
                    .predict(req);

            System.out.println("predict returned: " + response.getResultsList());

            assertEquals(1.0, response.getResults(0).getConfidence(), 0);

            assertEquals("classification for predict me! by model myModel",
                    response.getResults(0).getCategory());

            // verify getStatus
            status = manageModels.getModelStatus(GetStatusRequest.newBuilder()
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
            manageModels.unregisterModel(UnregisterModelRequest.newBuilder()
                    .setModelId(modelId).build());
        } finally {
            channel.shutdown();
        }
    }

    @Test
    public void loadFailureTest() throws Exception {
        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 8088)
                .usePlaintext().build();
        String modelId = "myModel_" + getClass().getSimpleName();
        ModelMeshBlockingStub manageModels = ModelMeshGrpc.newBlockingStub(channel);
        try {
            String errorMessage = "load should fail with this error";

            // add a model which should fail to load
            ModelStatusInfo statusInfo = manageModels.registerModel(RegisterModelRequest.newBuilder()
                    .setModelId(modelId).setModelInfo(ModelInfo.newBuilder().setType("ExampleType")
                            .setPath("FAIL_" + errorMessage).build()) // special prefix to trigger load in dummy runtime
                    .setLoadNow(true).setSync(true).build());

            System.out.println("registerModel returned: " + statusInfo);

            verifyFailureResponse(statusInfo, errorMessage);

            Thread.sleep(200);
            // Check that the response is consistent
            verifyFailureResponse(manageModels.getModelStatus(
                    GetStatusRequest.newBuilder().setModelId(modelId).build()), errorMessage);
        } finally {
            manageModels.unregisterModel(UnregisterModelRequest.newBuilder()
                    .setModelId(modelId).build());
            channel.shutdown();
        }
    }

    private void verifyFailureResponse(ModelStatusInfo statusInfo, String errorMessage) {
        assertEquals(ModelStatus.LOADING_FAILED, statusInfo.getStatus());
        assertEquals(1, statusInfo.getErrorsCount());
        assertTrue(statusInfo.getErrors(0).endsWith(errorMessage), statusInfo.getErrors(0));
        assertEquals(1, statusInfo.getModelCopyInfosCount());
        assertEquals(ModelStatus.LOADING_FAILED, statusInfo.getModelCopyInfos(0).getCopyStatus());
        assertFalse(Strings.isNullOrEmpty(statusInfo.getModelCopyInfos(0).getLocation()));
        long age = System.currentTimeMillis() - statusInfo.getModelCopyInfos(0).getTime();
        assertTrue(age >= 0L && age < 15000L, "time " + statusInfo.getModelCopyInfos(0).getTime()
                                             + " age " + age + "ms");
    }

    @Test
    public void multiModelTest() throws Exception {

        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 8088)
                .usePlaintext().build();
        try {
            ModelMeshBlockingStub manageModels = ModelMeshGrpc.newBlockingStub(channel);

            ExamplePredictorBlockingStub useModels = ExamplePredictorGrpc.newBlockingStub(channel);

            // add 3 models
            for (int i = 1; i <= 3; i++) {
                String modelId = "myModel-" + i;
                ModelStatusInfo statusInfo = manageModels.registerModel(RegisterModelRequest.newBuilder()
                        .setModelId(modelId).setModelInfo(ModelInfo.newBuilder().setType("ExampleType").build())
                        .setLoadNow(true).build());
                System.out.println("registerModel " + i + " returned: " + statusInfo.getStatus());
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

                manageModels.unregisterModel(UnregisterModelRequest.newBuilder()
                        .setModelId("myModel-i").build());
            }

            //TODO also test multi-vmodel

        } finally {
            channel.shutdown();
        }
    }
}
