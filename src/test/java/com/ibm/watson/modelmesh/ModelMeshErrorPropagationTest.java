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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Model-mesh unit tests
 */
public class ModelMeshErrorPropagationTest extends AbstractModelMeshClusterTest {

    @Override
    protected int replicaCount() {
        return 2;
    }

    static String CONSTRAINTS = "{\n" +
            "  \"my-type-1\": {\n" +
            "    \"required\": [\"my-label-1\"]\n" +
            "  }\n" +
            "}";

    @Override
    protected Map<String, String> extraEnvVars(String replicaId) {
        return !"9000".equals(replicaId) ? ImmutableMap.of("MM_TYPE_CONSTRAINTS", CONSTRAINTS)
                : ImmutableMap.of("MM_TYPE_CONSTRAINTS", CONSTRAINTS, "MM_LABELS", "my-label-1");
    }

    @Test
    public void errorPropagationTest() throws Exception {

        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 9000).usePlaintext().build();
        ManagedChannel channel2 = NettyChannelBuilder.forAddress("localhost", 9004).usePlaintext().build();
        try {
            ModelMeshBlockingStub manageModels = ModelMeshGrpc.newBlockingStub(channel);

            ExamplePredictorBlockingStub useModels = ExamplePredictorGrpc.newBlockingStub(channel);
            ExamplePredictorBlockingStub useModels2 = ExamplePredictorGrpc.newBlockingStub(channel2);

            // Add a model - with the type constraints it can only be loaded in one of the two instances
            String modelId = "myModel22";
            ModelStatusInfo statusInfo = manageModels.registerModel(RegisterModelRequest.newBuilder()
                    .setModelId(modelId).setModelInfo(ModelInfo.newBuilder().setType("my-type-1").build())
                    .setLoadNow(true).build());

            System.out.println("registerModel returned: " + statusInfo.getStatus());

            // call predict on the model to ensure that it's loaded and working
            PredictRequest req = PredictRequest.newBuilder().setText("predict me!").build();
            PredictResponse response = forModel(useModels, modelId).predict(req);
            assertEquals(1.0, response.getResults(0).getConfidence(), 0);
            assertEquals("classification for predict me! by model myModel22",
                    response.getResults(0).getCategory());

            // verify that there's only one copy loaded
            ModelStatusInfo status = manageModels.getModelStatus(GetStatusRequest.newBuilder()
                    .setModelId(modelId).build());
            assertEquals(ModelStatus.LOADED, status.getStatus());
            assertEquals(0, status.getErrorsCount());
            assertEquals(1, status.getModelCopyInfosCount());

            // Send a poison request to make the runtime return a specific error
            req = PredictRequest.newBuilder()
                    .setText("test:error:code=UNAVAILABLE:message=Fake prediction error message").build();

            // Ensure client recieves consistent error whichever instance the external request is sent to
            try {
                forModel(useModels, modelId).predict(req);
                fail("predict call should have failed");
            } catch (StatusRuntimeException sre) {
                assertExpectedException(sre);
            }

            try {
                forModel(useModels2, modelId).predict(req);
                fail("predict call should have failed");
            } catch (StatusRuntimeException sre) {
                assertExpectedException(sre);
            }

            // verify that there's still only one copy loaded
            status = manageModels.getModelStatus(GetStatusRequest.newBuilder()
                    .setModelId(modelId).build());
            assertEquals(ModelStatus.LOADED, status.getStatus());
            assertEquals(0, status.getErrorsCount());
            assertEquals(1, status.getModelCopyInfosCount());

            // delete
            manageModels.unregisterModel(UnregisterModelRequest.newBuilder()
                    .setModelId(modelId).build());
        } finally {
            channel.shutdown();
            channel2.shutdown();
        }
    }

    static void assertExpectedException(StatusRuntimeException sre) {
        Status status = Status.fromThrowable(sre);
        assertEquals(Status.Code.INTERNAL, status.getCode());
        assertEquals("ModelRuntime UNAVAILABLE: mmesh.ExamplePredictor/predict: UNAVAILABLE: Fake prediction error message",
                status.getDescription());
    }

}
