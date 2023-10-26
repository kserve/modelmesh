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

package com.ibm.watson.modelmesh.example;

import com.ibm.watson.modelmesh.SidecarModelMeshTest;
import com.ibm.watson.modelmesh.api.ModelInfo;
import com.ibm.watson.modelmesh.api.ModelMeshGrpc;
import com.ibm.watson.modelmesh.api.ModelMeshGrpc.ModelMeshBlockingStub;
import com.ibm.watson.modelmesh.api.ModelStatusInfo;
import com.ibm.watson.modelmesh.api.RegisterModelRequest;
import com.ibm.watson.modelmesh.example.api.ExamplePredictorGrpc;
import com.ibm.watson.modelmesh.example.api.ExamplePredictorGrpc.ExamplePredictorBlockingStub;
import com.ibm.watson.modelmesh.example.api.Predictor.PredictRequest;
import com.ibm.watson.modelmesh.example.api.Predictor.PredictResponse;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;

/**
 * Standalone test client example; logic is similar to that in {@link SidecarModelMeshTest}
 */
public class ExampleModelClient {

    static final Metadata.Key<String> MODEL_ID_META_KEY =
            Metadata.Key.of("mm-model-id", Metadata.ASCII_STRING_MARSHALLER);

    public static void main(String[] args) {
        String host;
        int port;
        if (args.length > 0) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        } else {
            host = "localhost";
            port = 8088;
        }

        ManagedChannel channel = NettyChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        try {
            ModelMeshBlockingStub manageModels = ModelMeshGrpc.newBlockingStub(channel);

            ExamplePredictorBlockingStub useModels = ExamplePredictorGrpc.newBlockingStub(channel);

            // add a model
            String modelId = "myModel";
            ModelStatusInfo statusInfo = manageModels.registerModel(RegisterModelRequest.newBuilder()
                    .setModelId(modelId).setModelInfo(ModelInfo.newBuilder()
                            .setType("ExampleType").build())
                    .setLoadNow(true).build());

            System.out.println("addModel returned: " + statusInfo.getStatus());

            // call predict on the model
            PredictResponse response = forModel(useModels, modelId)
                    .predict(PredictRequest.newBuilder().setText("predict me!").build());

            System.out.println("predict returned: " + response.getResultsList());
        } finally {
            channel.shutdown();
        }
    }

    public static <T extends AbstractStub<T>> T forModel(T stub, String modelId) {
        Metadata headers = new Metadata();
        headers.put(MODEL_ID_META_KEY, modelId);
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

}
