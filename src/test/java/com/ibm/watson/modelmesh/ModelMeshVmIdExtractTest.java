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
import com.ibm.watson.modelmesh.api.DeleteVModelRequest;
import com.ibm.watson.modelmesh.api.ModelInfo;
import com.ibm.watson.modelmesh.api.ModelMeshGrpc;
import com.ibm.watson.modelmesh.api.ModelMeshGrpc.ModelMeshBlockingStub;
import com.ibm.watson.modelmesh.api.RegisterModelRequest;
import com.ibm.watson.modelmesh.api.SetVModelRequest;
import com.ibm.watson.modelmesh.api.UnregisterModelRequest;
import com.ibm.watson.modelmesh.example.api.ExamplePredictorGrpc;
import com.ibm.watson.modelmesh.example.api.ExamplePredictorGrpc.ExamplePredictorBlockingStub;
import com.ibm.watson.modelmesh.example.api.Predictor.PredictRequest;
import com.ibm.watson.modelmesh.example.api.Predictor.PredictResponse;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 *
 */
public class ModelMeshVmIdExtractTest extends AbstractModelMeshClusterTest {

    @Override
    protected int replicaCount() {
        return 1;
    }

    @Override
    protected Map<String, String> extraEnvVars() {
        return ImmutableMap.of("MM_DATAPLANE_CONFIG", "{"
                                                      + "\"rpcConfigs\": {"
                                                      +
                                                      "\"mmesh.ExamplePredictor/predict\": { \"idExtractionPath\": [1], \"vModelId\": true }"
                                                      + "}"
                                                      + "}");
    }

    @Override
    protected Map<String, String> extraRuntimeEnvVars() {
        return ImmutableMap.of("RS_METHOD_INFOS", "mmesh.ExamplePredictor/predict=1");
    }

    @Test
    public void idExtractionTest() throws Exception {

        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 9000).usePlaintext().build();
        try {
            ModelMeshBlockingStub manageModels = ModelMeshGrpc.newBlockingStub(channel);
            ExamplePredictorBlockingStub useModels = ExamplePredictorGrpc.newBlockingStub(channel);

            // add a model
            String modelId = "myModel", vmodelId = "myVmodel";
            manageModels.registerModel(RegisterModelRequest.newBuilder()
                    .setModelId(modelId).setModelInfo(ModelInfo.newBuilder().setType("ExampleType").build())
                    .setLoadNow(true).build());

            manageModels.setVModel(SetVModelRequest.newBuilder().setVModelId(vmodelId)
                    .setTargetModelId(modelId).build());

            PredictRequest req = PredictRequest.newBuilder().setText("predict me!").build();
            PredictResponse response = forVModel(useModels, vmodelId).predict(req);
            assertEquals("classification for predict me! by model myModel",
                    response.getResults(0).getCategory());
            // verify that the model id was injected as the model name
            // (which is reflected back as the second "category" in the result)
            assertEquals(modelId, response.getResults(1).getCategory());

            //TODO check that putting model id in payload will trigger notfound

            req = PredictRequest.newBuilder()
                    .setText("predict me!").setModelName(vmodelId).build();
            response = useModels.predict(req);
            assertEquals("classification for predict me! by model myModel",
                    response.getResults(0).getCategory());
            assertEquals(modelId, response.getResults(1).getCategory());
            try {
                forVModel(useModels, vmodelId).multiPredict(req);
                fail("Attempt to call restricted method should have failed");
            } catch (Exception e) {
                System.out.println(e);
                assertEquals(Status.Code.UNIMPLEMENTED, Status.fromThrowable(e).getCode());
            }

            // delete
            manageModels.deleteVModel(DeleteVModelRequest.newBuilder().setVModelId(vmodelId).build());
            manageModels.unregisterModel(UnregisterModelRequest.newBuilder()
                    .setModelId(modelId).build());
        } finally {
            channel.shutdown();
        }
    }
}
