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

import com.ibm.watson.modelmesh.api.ModelInfo;
import com.ibm.watson.modelmesh.api.ModelMeshGrpc;
import com.ibm.watson.modelmesh.api.ModelMeshGrpc.ModelMeshBlockingStub;
import com.ibm.watson.modelmesh.api.ModelRuntimeGrpc;
import com.ibm.watson.modelmesh.api.ModelStatusInfo.ModelStatus;
import com.ibm.watson.modelmesh.api.RegisterModelRequest;
import com.ibm.watson.modelmesh.api.UnloadModelRequest;
import com.ibm.watson.modelmesh.api.UnregisterModelRequest;
import com.ibm.watson.modelmesh.example.api.ExamplePredictorGrpc;
import com.ibm.watson.modelmesh.example.api.ExamplePredictorGrpc.ExamplePredictorBlockingStub;
import com.ibm.watson.modelmesh.example.api.Predictor.PredictRequest;
import com.ibm.watson.modelmesh.example.api.Predictor.PredictResponse;
import io.grpc.ManagedChannel;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static java.lang.System.nanoTime;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test recovery behaviour where model runtime unexpectedly returns NOT_FOUND from an inference request
 * (typically because it restarted).
 */
public class ModelMeshRefreshMissingModelTest extends AbstractModelMeshClusterTest {

    @Override
    protected int replicaCount() {
        return 1;
    }

    @Test
    public void handleMissingModelTest() throws Exception {

        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 9000).usePlaintext().build();
        try {
            ModelMeshBlockingStub manageModels = ModelMeshGrpc.newBlockingStub(channel);
            ExamplePredictorBlockingStub useModels = ExamplePredictorGrpc.newBlockingStub(channel);

            // Add a model
            String modelId = "myModel";
            assertEquals(ModelStatus.LOADED, manageModels.registerModel(RegisterModelRequest.newBuilder()
                    .setModelId(modelId).setModelInfo(ModelInfo.newBuilder().setType("ExampleType").build())
                    .setLoadNow(true).setSync(true).build()).getStatus());

            // Check we can call it
            PredictRequest req = PredictRequest.newBuilder()
                    .setText("predict me!").setModelName("n/a").build();
            PredictResponse response;
            try (Timed t = new Timed("First inference")) {
                response = forModel(useModels, modelId).predict(req);
            }
            assertEquals("classification for predict me! by model myModel",
                    response.getResults(0).getCategory());

            for (int i = 1; i <= 2; i++) {
                System.out.println("Starting iteration " + i);
                // Perform a sneaky out-of-band unload of the model here from
                // the model server process, model-mesh isn't aware of
                ManagedChannel mrChannel = NettyChannelBuilder.forAddress("localhost", 9000 + 2).usePlaintext().build();
                ModelRuntimeGrpc.ModelRuntimeBlockingStub mrStub = ModelRuntimeGrpc.newBlockingStub(mrChannel);
                // This is a blocking call, if it doesn't throw then the unload was successful.
                try {
                    mrStub.unloadModel(UnloadModelRequest.newBuilder().setModelId("myModel").build());
                } catch (StatusRuntimeException sre) {
                    if (sre.getStatus().getCode() != Code.NOT_FOUND) throw sre;
                }

                // Try to call it again - model-mesh should catch the NOT_FOUND response from the runtime,
                // update its local cache, attempt to re-load the model and then retry the request
                try (Timed t = new Timed("Inference after unload")) {
                    response = forModel(useModels, modelId).predict(req);
                }
                assertEquals("classification for predict me! by model myModel",
                        response.getResults(0).getCategory());
                
                Thread.sleep(2500);
            }

            // Clean up
            manageModels.unregisterModel(UnregisterModelRequest.newBuilder()
                    .setModelId(modelId).build());
        } finally {
            channel.shutdown();
        }
    }
    
    
    static final class Timed implements AutoCloseable {
        final long startTime = nanoTime();
        final String taskName;

        Timed(String taskName) { this.taskName = taskName; }

        @Override
        public void close() {
            System.out.println(">>> " + taskName + " took "
                + TimeUnit.MILLISECONDS.convert(nanoTime() - startTime, TimeUnit.NANOSECONDS) + "ms");
        }
    }
}
