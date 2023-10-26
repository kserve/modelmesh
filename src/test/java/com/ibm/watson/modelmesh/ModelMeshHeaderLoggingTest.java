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
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test request header logging function
 */
public class ModelMeshHeaderLoggingTest extends AbstractModelMeshClusterTest {

    private static final String ASCII_HEADER_NAME = "ascii-header";
    private static final String UTF8_HEADER_NAME = "utf8-header-bin";
    private static final String EXTRA_HEADER_NAME = "extra-header";

    @Override
    protected int replicaCount() {
        return 1;
    }

    @Override
    protected Map<String, String> extraEnvVars() {
        return ImmutableMap.of(
                // Ensure each request produces its own log message
                "MM_LOG_EACH_INVOKE", "true",
                "MM_LOG_REQUEST_HEADERS",
                    String.format("{\"%s\":\"ah\", \"%s\":\"uh\", \"%s\":\"eh\"}",
                        ASCII_HEADER_NAME, UTF8_HEADER_NAME, EXTRA_HEADER_NAME)
        );
    }

    @Override
    protected boolean inheritIo() {
        return false;
    }

    @Test
    public void headerLoggingTest() throws Exception {

        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 9000).usePlaintext().build();
        try {
            ModelMeshBlockingStub manageModels = ModelMeshGrpc.newBlockingStub(channel);
            ExamplePredictorBlockingStub useModels = ExamplePredictorGrpc.newBlockingStub(channel);

            // add a model
            String modelId = "myModel";
            manageModels.registerModel(RegisterModelRequest.newBuilder()
                    .setModelId(modelId).setModelInfo(ModelInfo.newBuilder().setType("ExampleType").build())
                    .setLoadNow(true).build());
            PredictRequest req = PredictRequest.newBuilder()
                    .setText("predict me!")
                    .setModelName("should be overridden")
                    .build();
                    
            // Set up headers
            Key<String> asciiHeader = Key.of(ASCII_HEADER_NAME, Metadata.ASCII_STRING_MARSHALLER);
            Key<String> utf8Header = Key.of(UTF8_HEADER_NAME, GrpcSupport.UTF8_MARSHALLER);
            Key<String> unloggedHeader = Key.of("unlogged-header", Metadata.ASCII_STRING_MARSHALLER);

            String asciiVal = "my-ascii-value", utf8Val = "my-utf8-value-\u00ea\u00f1\u00fc";

            Metadata headers = new Metadata();
            headers.put(MODEL_ID_META_KEY, modelId);
            headers.put(utf8Header, utf8Val);
            headers.put(asciiHeader, asciiVal);
            // We'll make sure this one *isn't* logged since it wasn't included in the config
            headers.put(unloggedHeader, "my-unlogged-value"); 

            PredictResponse response = useModels.withInterceptors(
                    MetadataUtils.newAttachHeadersInterceptor(headers)).predict(req);
            assertEquals("classification for predict me! by model myModel",
                    response.getResults(0).getCategory());

            // Verify expected log output here
            BufferedReader br = new BufferedReader(new InputStreamReader(getPodClosers()[0].getInputStream()));
            String expectedMapOutput = String.format("{ah=%s, uh=%s}", asciiVal, utf8Val);
            while (br.ready()) {
                String line = br.readLine();
                System.out.println("MMLOG>> " + line);
                if (line.contains("invoking model runtime")) {
                    // Using pattern logger with context map included at end of log message,
                    // would be better to configure json logger instead
                    assertTrue(line.endsWith(" " + expectedMapOutput),
                        "request log line does not contain expected context map "
                            + expectedMapOutput + ": " + line);
                }
            }

            // delete
            manageModels.unregisterModel(UnregisterModelRequest.newBuilder()
                    .setModelId(modelId).build());
        } finally {
            channel.shutdown();
        }
    }
}
