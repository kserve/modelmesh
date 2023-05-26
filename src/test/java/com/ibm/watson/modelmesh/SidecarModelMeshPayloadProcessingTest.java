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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Model-mesh test for payload processing
 */
public class SidecarModelMeshPayloadProcessingTest extends SingleInstanceModelMeshTest {

    @BeforeEach
    public void initialize() throws Exception {
        System.setProperty(ModelMeshEnvVars.MM_PAYLOAD_PROCESSORS, "http://localhost:8080/consumer/kserve/v2");
        super.initialize();
    }

    @Test
    public void testPayloadProcessing() {
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

            // delete
            manageModels.unregisterModel(UnregisterModelRequest.newBuilder()
                                                 .setModelId(modelId).build());
        } finally {
            channel.shutdown();
        }
    }

}
