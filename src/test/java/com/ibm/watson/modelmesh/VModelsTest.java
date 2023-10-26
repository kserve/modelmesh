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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.ibm.watson.modelmesh.api.DeleteVModelRequest;
import com.ibm.watson.modelmesh.api.GetStatusRequest;
import com.ibm.watson.modelmesh.api.GetVModelStatusRequest;
import com.ibm.watson.modelmesh.api.ModelInfo;
import com.ibm.watson.modelmesh.api.ModelMeshGrpc;
import com.ibm.watson.modelmesh.api.ModelMeshGrpc.ModelMeshBlockingStub;
import com.ibm.watson.modelmesh.api.ModelStatusInfo;
import com.ibm.watson.modelmesh.api.ModelStatusInfo.ModelStatus;
import com.ibm.watson.modelmesh.api.RegisterModelRequest;
import com.ibm.watson.modelmesh.api.SetVModelRequest;
import com.ibm.watson.modelmesh.api.UnregisterModelRequest;
import com.ibm.watson.modelmesh.api.VModelStatusInfo;
import com.ibm.watson.modelmesh.api.VModelStatusInfo.VModelStatus;
import com.ibm.watson.modelmesh.example.api.ExamplePredictorGrpc;
import com.ibm.watson.modelmesh.example.api.ExamplePredictorGrpc.ExamplePredictorBlockingStub;
import com.ibm.watson.modelmesh.example.api.Predictor.PredictRequest;
import com.ibm.watson.modelmesh.example.api.Predictor.PredictResponse;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Model-mesh vmodels unit tests
 */
public class VModelsTest extends SingleInstanceModelMeshTest {

    @Test
    public void vModelsTest() throws Exception {

        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 8088)
                .usePlaintext().build();
        try {
            ModelMeshBlockingStub manageModels = ModelMeshGrpc.newBlockingStub(channel);

            ExamplePredictorBlockingStub useModels = ExamplePredictorGrpc.newBlockingStub(channel);

            // add a model
            String modelId = "myModel";
            ModelStatusInfo statusInfo = manageModels.registerModel(RegisterModelRequest.newBuilder()
                    .setModelId(modelId).setModelInfo(ModelInfo.newBuilder().setType("ExampleType").build())
                    .setLoadNow(true).build());

            System.out.println("registerModel returned: " + statusInfo.getStatus());

            // call predict on the model
            PredictRequest req = PredictRequest.newBuilder().setText("predict me!").build();
            PredictResponse response = forModel(useModels, modelId)
                    .predict(req);

            System.out.println("predict returned: " + response.getResultsList());

            assertEquals(1.0, response.getResults(0).getConfidence(), 0);

            assertEquals("classification for predict me! by model myModel",
                    response.getResults(0).getCategory());

//            manageModels.unregisterModel(UnregisterModelRequest.newBuilder()
//                    .setModelId(modelId).build());

            manageModels.deleteVModel(DeleteVModelRequest.newBuilder()
                    .setVModelId("test-vmodel-a").build());

            VModelStatusInfo vmsi = manageModels.setVModel(SetVModelRequest.newBuilder()
                    .setVModelId("test-vmodel-a").setTargetModelId(modelId).build());

            System.out.println("add vmodel = " + vmsi);

            try {
                // should fail
                manageModels.unregisterModel(UnregisterModelRequest.newBuilder()
                        .setModelId(modelId).build());
                fail("should not be able to delete model referenced by vmodel");
            } catch (StatusRuntimeException sre) {
                assertEquals(Code.FAILED_PRECONDITION, sre.getStatus().getCode());
            }

            // call predict on the vmodel
            PredictResponse response2 = forVModel(useModels, "test-vmodel-a")
                    .predict(PredictRequest.newBuilder().setText("vpredict me!").build());

            System.out.println("vpredict returned: " + response2.getResultsList());

            // sync == true => this call should block until transition complete
            vmsi = manageModels.setVModel(SetVModelRequest.newBuilder()
                    .setVModelId("test-vmodel-a").setTargetModelId("myNextModel3")
                    .setExpectedTargetModelId(modelId) // expected value should match - so this should succeed
                    .setModelInfo(ModelInfo.newBuilder().setType("ExampleOtherType").build())
                    .setAutoDeleteTargetModel(true).setUpdateOnly(true)
                    .setSync(true).build());

            System.out.println("set vmodel targ = " + vmsi);

            assertEquals("myNextModel3", vmsi.getTargetModelId());
            assertEquals("myNextModel3", vmsi.getActiveModelId());
            assertEquals(ModelStatus.LOADED, vmsi.getActiveModelStatus().getStatus());

            ModelStatusInfo msi = manageModels.getModelStatus(GetStatusRequest.newBuilder()
                    .setModelId(modelId).build());
            assertNotEquals(ModelStatus.NOT_FOUND, msi.getStatus());

            // should succeed now
            manageModels.unregisterModel(UnregisterModelRequest.newBuilder()
                    .setModelId(modelId).build());

            // should succeed because same values
            vmsi = manageModels.setVModel(SetVModelRequest.newBuilder()
                    .setVModelId("test-vmodel-a").setTargetModelId("myNextModel3")
                    .setModelInfo(ModelInfo.newBuilder().setType("ExampleOtherType").build())
                    .setUpdateOnly(true).setAutoDeleteTargetModel(true).build());

            System.out.println("set vmodel targ2 = " + vmsi);

            manageModels.registerModel(RegisterModelRequest.newBuilder()
                    .setModelId("another-model").setModelInfo(ModelInfo.newBuilder()
                            .setType("ExampleType").build()).build());

            // these should fail because expected doesn't match
            try {
                manageModels.setVModel(SetVModelRequest.newBuilder()
                        .setVModelId("test-vmodel-a").setTargetModelId("another-model")
                        .setAutoDeleteTargetModel(true)
                        .setExpectedTargetModelId("wrong-existing-model").build());
                fail("should fail due to expected model mismatch");
            } catch (StatusRuntimeException sre) {
                assertEquals(Code.FAILED_PRECONDITION, sre.getStatus().getCode());
            }

            // this should fail because it doesn't exist
            try {
                manageModels.setVModel(SetVModelRequest.newBuilder()
                        .setVModelId("test-vmodel-b").setTargetModelId("another-model")
                        .setAutoDeleteTargetModel(true)
                        .setExpectedTargetModelId("wrong-existing-model").build());
                fail("should fail due to expected model mismatch");
            } catch (StatusRuntimeException sre) {
                assertEquals(Code.FAILED_PRECONDITION, sre.getStatus().getCode());
            }

            // sync == false => will return before transitioned
            vmsi = manageModels.setVModel(SetVModelRequest.newBuilder()
                    .setVModelId("test-vmodel-a").setTargetModelId("myNextModel4")
                    .setModelInfo(ModelInfo.newBuilder().setType("ExampleOtherType").build())
                    .setUpdateOnly(true).build());

            System.out.println("set vmodel targ4 = " + vmsi);

            assertEquals("myNextModel4", vmsi.getTargetModelId());
            assertEquals("myNextModel3", vmsi.getActiveModelId());
            assertEquals(ModelStatus.LOADED, vmsi.getActiveModelStatus().getStatus());

            VModelStatusInfo vmsi2 = manageModels.getVModelStatus(GetVModelStatusRequest.newBuilder()
                    .setVModelId("test-vmodel-a").build());

            // Model load is initiated asynchronously, so status info returned from
            // initial setVModel might not contain details of copy load location
            assertEquals(clearTargetCopyInfo(vmsi), clearTargetCopyInfo(vmsi2));

            // should succeed because target matches, sync == true => will block
            vmsi = manageModels.setVModel(SetVModelRequest.newBuilder()
                    .setVModelId("test-vmodel-a").setTargetModelId("myNextModel4")
                    .setUpdateOnly(true).setSync(true).build());

            // after the transition, prior model should have been auto-deleted
            msi = manageModels.getModelStatus(GetStatusRequest.newBuilder()
                    .setModelId("myNextModel3").build());
            assertEquals(ModelStatus.NOT_FOUND, msi.getStatus());

            //TODO test transition to another model, ensure myNextModel4 remains (because wasn't auto-deleted)

            manageModels.deleteVModel(DeleteVModelRequest.newBuilder()
                    .setVModelId("test-vmodel-a").build());

            vmsi = manageModels.getVModelStatus(GetVModelStatusRequest.newBuilder()
                    .setVModelId("test-vmodel-a").build());
            System.out.println("vmsi after delete = " + vmsi);
            assertEquals(ModelStatus.NOT_FOUND, vmsi.getActiveModelStatus().getStatus());
            assertEquals("", vmsi.getTargetModelId());
            assertEquals("", vmsi.getActiveModelId());

            // we didn't select auto-delete so this should still be there
            msi = manageModels.getModelStatus(GetStatusRequest.newBuilder()
                    .setModelId("myNextModel4").build());
            assertNotEquals(ModelStatus.NOT_FOUND, msi.getStatus());

            //TODO test that auto-delete models *do* get deleted when corresp. vmodel is deleted

            manageModels.unregisterModel(UnregisterModelRequest.newBuilder()
                    .setModelId("myNextModel4").build());

        } finally {
            channel.shutdown();
        }
    }


    @Test
    public void vModelOwnerTest() throws Exception {

        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 8088)
                .usePlaintext().build();
        try {
            ModelMeshBlockingStub manageModels = ModelMeshGrpc.newBlockingStub(channel);

            // add a model
            String modelId = "myModel";
            ModelStatusInfo statusInfo = manageModels.registerModel(RegisterModelRequest.newBuilder()
                    .setModelId(modelId).setModelInfo(ModelInfo.newBuilder().setType("ExampleType").build())
                    .setLoadNow(true).build());

            System.out.println("registerModel returned: " + statusInfo.getStatus());

            VModelStatusInfo vmsi = manageModels.setVModel(SetVModelRequest.newBuilder()
                    .setVModelId("test-vmodel-a").setTargetModelId(modelId).build());

            assertEquals("", vmsi.getOwner());

            VModelStatusInfo vmsi2 = manageModels.setVModel(SetVModelRequest.newBuilder()
                    .setVModelId("test-vmodel-owner1").setTargetModelId(modelId).setOwner("owner1").build());
            assertEquals("owner1", vmsi2.getOwner());

            // Same set should succeed with same owner
            vmsi2 = manageModels.setVModel(SetVModelRequest.newBuilder()
                    .setVModelId("test-vmodel-owner1").setTargetModelId(modelId).setOwner("owner1").build());
            assertEquals("owner1", vmsi2.getOwner());

            // Should succeed with no owner stipulation
            vmsi2 = manageModels.setVModel(SetVModelRequest.newBuilder()
                    .setVModelId("test-vmodel-owner1").setTargetModelId(modelId).build());
            assertEquals("owner1", vmsi2.getOwner());

            // Should fail with different owner //TODO
            try {
                manageModels.setVModel(SetVModelRequest.newBuilder()
                        .setVModelId("test-vmodel-owner1").setTargetModelId(modelId).setOwner("owner2").build());
            } catch (StatusRuntimeException sre) {
                assertEquals(Code.ALREADY_EXISTS, sre.getStatus().getCode());
                assertEquals("vmodel test-vmodel-owner1"
                             + " already exists with different owner \"owner1\"", sre.getStatus().getDescription());
            }

            // Should return with owner if none stipulated
            vmsi2 = manageModels.getVModelStatus(GetVModelStatusRequest.newBuilder()
                    .setVModelId("test-vmodel-owner1").build());
            assertEquals(VModelStatus.DEFINED, vmsi2.getStatus());
            assertEquals("owner1", vmsi2.getOwner());

            // Should return with owner if correct owner stipulated
            VModelStatusInfo vmsi22 = manageModels.getVModelStatus(GetVModelStatusRequest.newBuilder()
                    .setVModelId("test-vmodel-owner1").setOwner("owner1").build());
            assertEquals(vmsi2, vmsi22);

            // Should return NOT_FOUND if incorrect owner stipulated
            VModelStatusInfo vmsi23 = manageModels.getVModelStatus(GetVModelStatusRequest.newBuilder()
                    .setVModelId("test-vmodel-owner1").setOwner("owner2").build());
            assertEquals(VModelStatus.NOT_FOUND, vmsi23.getStatus());

            // Delete call should succeed but not take effect if no owner stipulated
            manageModels.deleteVModel(DeleteVModelRequest.newBuilder().setVModelId("test-vmodel-owner1")
                    .setOwner("owner2").build());
            // verify still there
            vmsi2 = manageModels.getVModelStatus(GetVModelStatusRequest.newBuilder()
                    .setVModelId("test-vmodel-owner1").build());
            assertEquals(VModelStatus.DEFINED, vmsi2.getStatus());
            assertEquals("owner1", vmsi2.getOwner());

            // Should succeed and take effect if correct owner is stipulated
            manageModels.deleteVModel(DeleteVModelRequest.newBuilder().setVModelId("test-vmodel-owner1")
                    .setOwner("owner1").build());
            // verify not found
            vmsi2 = manageModels.getVModelStatus(GetVModelStatusRequest.newBuilder()
                    .setVModelId("test-vmodel-owner1").build());
            assertEquals(VModelStatus.NOT_FOUND, vmsi2.getStatus());

            //cleanup
            manageModels.deleteVModel(DeleteVModelRequest.newBuilder().setVModelId("test-vmodel-a").build());
            manageModels.unregisterModel(UnregisterModelRequest.newBuilder().setModelId(modelId).build());

        } finally {
            channel.shutdown();
        }
    }

    private static VModelStatusInfo clearTargetCopyInfo(VModelStatusInfo vmsi) {
        return VModelStatusInfo.newBuilder(vmsi).setTargetModelStatus(
                ModelStatusInfo.newBuilder(vmsi.getTargetModelStatus())
                        .clearModelCopyInfos()).build();
    }

    static final Metadata.Key<String> VMODEL_ID_META_KEY =
            Metadata.Key.of("mm-vmodel-id", Metadata.ASCII_STRING_MARSHALLER);

    public static <T extends AbstractStub<T>> T forVModel(T stub, String modelId) {
        Metadata headers = new Metadata();
        headers.put(VMODEL_ID_META_KEY, modelId);
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }
}
