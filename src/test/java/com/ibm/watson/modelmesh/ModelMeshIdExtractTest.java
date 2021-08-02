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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.ibm.watson.modelmesh.api.ModelInfo;
import com.ibm.watson.modelmesh.api.ModelMeshGrpc;
import com.ibm.watson.modelmesh.api.ModelMeshGrpc.ModelMeshBlockingStub;
import com.ibm.watson.modelmesh.api.RegisterModelRequest;
import com.ibm.watson.modelmesh.api.UnregisterModelRequest;
import com.ibm.watson.modelmesh.example.api.ExamplePredictorGrpc;
import com.ibm.watson.modelmesh.example.api.ExamplePredictorGrpc.ExamplePredictorBlockingStub;
import com.ibm.watson.modelmesh.example.api.Predictor.PredictRequest;
import com.ibm.watson.modelmesh.example.api.Predictor.PredictResponse;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 *
 */
public class ModelMeshIdExtractTest extends AbstractModelMeshClusterTest {

    @Override
    protected int replicaCount() {
        return 1;
    }

    @Override
    protected Map<String, String> extraEnvVars() {
        return ImmutableMap.of("MM_DATAPLANE_CONFIG", "{"
                                                      + "\"rpcConfigs\": {"
                                                      +
                                                      "\"mmesh.ExamplePredictor/predict\": { \"idExtractionPath\": [1] }"
                                                      + "},"
                                                      + "\"allowOtherRpcs\": true"
                                                      + "}");
    }

    @Override
    protected Map<String, String> extraRuntimeEnvVars() {
        return ImmutableMap.of("RS_METHOD_INFOS", "mmesh.ExamplePredictor/predict=1",
                "RS_ALLOW_ANY", "true");
    }

    @Test
    public void idExtractionTest() throws Exception {

        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 9000).usePlaintext().build();
        try {
            ModelMeshBlockingStub manageModels = ModelMeshGrpc.newBlockingStub(channel);
            ExamplePredictorBlockingStub useModels = ExamplePredictorGrpc.newBlockingStub(channel);

            // add a model
            String modelId = "myModel";
            manageModels.registerModel(RegisterModelRequest.newBuilder()
                    .setModelId(modelId).setModelInfo(ModelInfo.newBuilder().setType("ExampleType").build())
                    .setLoadNow(true).build());
            PredictRequest req = PredictRequest.newBuilder().setText("predict me!").build();
            PredictResponse response = forModel(useModels, modelId).predict(req);
            assertEquals("classification for predict me! by model myModel",
                    response.getResults(0).getCategory());
            // verify that the model id was injected as the model name
            // (which is reflected back as the second "category" in the result)
            assertEquals(modelId, response.getResults(1).getCategory());

            req = PredictRequest.newBuilder()
                    .setText("predict me!").setModelName(modelId).build();
            response = useModels.predict(req);
            assertEquals("classification for predict me! by model myModel",
                    response.getResults(0).getCategory());
            assertEquals(modelId, response.getResults(1).getCategory());

            // model id in message takes precedence
            response = forModel(useModels, "should be ignored").predict(req);
            assertEquals("classification for predict me! by model myModel",
                    response.getResults(0).getCategory());
            assertEquals(modelId, response.getResults(1).getCategory());

            req = PredictRequest.newBuilder().setText("predict me!").build();
            response = forModel(useModels, modelId).multiPredict(req)
                    .getPerModelResultsOrThrow(modelId);
            assertEquals("classification for predict me! by model myModel",
                    response.getResults(0).getCategory());
            assertEquals("", response.getResults(1).getCategory());

            // delete
            manageModels.unregisterModel(UnregisterModelRequest.newBuilder()
                    .setModelId(modelId).build());
        } finally {
            channel.shutdown();
        }
    }
}
