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
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ibm.watson.modelmesh.ModelMeshEnvVars.LOAD_FAILURE_EXPIRY_ENV_VAR;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test load failure expiry while model is receiving continuous stream of inference requests
 */
public class ModelMeshFailureExpiryTest extends SingleInstanceModelMeshTest {

    @Override
    @BeforeEach
    public void initialize() throws Exception {
        System.setProperty("tas.janitor_freq_secs", "1"); // set janitor to run every second
        System.setProperty(LOAD_FAILURE_EXPIRY_ENV_VAR, "2000"); // expire failures in 2 seconds
        try {
            super.initialize();
        } finally {
            System.clearProperty("tas.janitor_freq_secs");
            System.clearProperty(LOAD_FAILURE_EXPIRY_ENV_VAR);
        }
    }

    @Test
    public void failureExpiryTest() throws Exception {
        ExecutorService es = Executors.newSingleThreadExecutor();

        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 8088)
                .usePlaintext().build();
        String modelId = "myFailExpireModel";
        ModelMeshBlockingStub manageModels = ModelMeshGrpc.newBlockingStub(channel);

        final AtomicBoolean stop = new AtomicBoolean();
        try {
            ExamplePredictorBlockingStub useModels = ExamplePredictorGrpc.newBlockingStub(channel);

            String errorMessage = "load should fail with this error";

            // add a model which should fail to load
            ModelStatusInfo statusInfo = manageModels.registerModel(RegisterModelRequest.newBuilder()
                    .setModelId(modelId).setModelInfo(ModelInfo.newBuilder().setType("ExampleType")
                            .setPath("FAIL_" + errorMessage).build()) // special prefix to trigger load in dummy runtime
                    .setLoadNow(true).setSync(true).build());

            System.out.println("registerModel returned: " + statusInfo);
            assertEquals(ModelStatus.LOADING_FAILED, statusInfo.getStatus());


            final PredictRequest req = PredictRequest.newBuilder().setText("predict me!").build();

            es.execute(() -> {
                while (!stop.get()) try {
                    forModel(useModels, modelId).predict(req);
                } catch (StatusRuntimeException sre) {
                    // ignore
                } finally {
                    try {
                        Thread.sleep(400L);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            // call predict on the model
            try {
                forModel(useModels, modelId).predict(req);
                fail("failed model should fail predict");
            } catch (StatusRuntimeException sre) {
                assertTrue(sre.getStatus().getDescription().endsWith(errorMessage));
            }

            Thread.sleep(1000);

            // not expired yet
            try {
                forModel(useModels, modelId).predict(req);
                fail("failed model should fail predict");
            } catch (StatusRuntimeException sre) {
                assertTrue(sre.getStatus().getDescription().endsWith(errorMessage));
            }

            Thread.sleep(3000);

            // failure should now be expired and it should load successfully second time
            // per logic in dummy Model class

            PredictResponse response = forModel(useModels, modelId).predict(req);
            System.out.println("predict returned: " + response.getResultsList());
            assertEquals(1.0, response.getResults(0).getConfidence(), 0);
            assertEquals("classification for predict me! by model " + modelId,
                    response.getResults(0).getCategory());

        } finally {
            stop.set(true);
            manageModels.unregisterModel(UnregisterModelRequest.newBuilder()
                    .setModelId(modelId).build());
            channel.shutdown();
            es.shutdown();
        }
    }

}
