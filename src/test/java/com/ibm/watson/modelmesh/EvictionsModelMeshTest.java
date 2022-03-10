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

import com.google.common.collect.ImmutableSet;
import com.ibm.watson.modelmesh.api.*;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class EvictionsModelMeshTest extends SingleInstanceModelMeshTest {

    // The dummy ExampleModelRuntime reports:
    //    capacity of 1024MiB, default model size of 50MiB, max loading concurrency of 6
    // which results in an unload buffer of size 75MiB and effective capacity of
    // 1024 - 75 = 949MiB

    @Test
    public void basicEvictionTest() throws Exception {
        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 8088)
            .usePlaintext().build();
        ModelMeshGrpc.ModelMeshBlockingStub manageModels = ModelMeshGrpc.newBlockingStub(channel);
        try {

            // Template for the models that we'll register
            // They will take 20ms to load and 1 second to unload (opposite of "real" behaviour
            // but will allow us to test unload buffering)
            RegisterModelRequest.Builder newModel = RegisterModelRequest.newBuilder()
                .setModelInfo(ModelInfo.newBuilder()
                    .setPath("SIZE_"+(50*1024*1024)+"_20_1000")
                    .setType("ExampleType").build())
                .setLoadNow(true);

            // Add 17 models which should leave room for one more
            for (int i = 0; i < 17; i++) {
                manageModels.registerModel(newModel.setModelId("myModel" + i).build());
                Thread.sleep(10L);
            }
            // Add 18th model and wait for it to finish loading
            ModelStatusInfo status = manageModels.registerModel(newModel.setModelId("myModel17").setSync(true).build());
            assertEquals(ModelStatusInfo.ModelStatus.LOADED, status.getStatus());
            // Verify that all 18 report loaded
            for (int i = 0; i < 18; i++) {
                assertEquals(ModelStatusInfo.ModelStatus.LOADED, status(manageModels, "myModel" + i));
            }

            // Add 19th model which should take total capacity to 950 > 949 and so evict the oldest one (i == 0)
            // note this is still a sync request and will wait for the new model to load.
            long t1 = System.nanoTime();
            status = manageModels.registerModel(newModel.setModelId("myModel18").build());
            long tookMillis = (System.nanoTime() - t1) / 1000_000L;
            assertEquals(ModelStatusInfo.ModelStatus.LOADED, status.getStatus());

            // Ensure that the load completes quickly - even though the cache is full, the unload buffer
            // should be used here for the unload of the evicted model
            assertTrue(tookMillis > 15 && tookMillis < 50, "took " + tookMillis);
            System.out.println("Load took " + tookMillis + " ms");

            // First model should immediately become "not loaded" (it will actually be unloading at this point)
            assertEquals(ModelStatusInfo.ModelStatus.NOT_LOADED, status(manageModels, "myModel0"));

            // Remainder should all still be loaded
            for (int i = 1; i < 18; i++) {
                assertEquals(ModelStatusInfo.ModelStatus.LOADED, status(manageModels, "myModel" + i));
            }

            // Now we add 20th and 21st models which will evict two more models (with i==1,2). However the unload
            // buffer should still be occupied at this point by myModel0 (because unload time is 1sec) leaving
            // sufficient room for only one more unload. So the load of myModel20 should be held up until the
            // original unload completes (and loads here are much faster than unloads).
            // Note that these register requests still have sync set to true so they block while loading.
            status = manageModels.registerModel(newModel.setModelId("myModel19").build());
            assertEquals(ModelStatusInfo.ModelStatus.NOT_LOADED, status(manageModels, "myModel1"));
            assertEquals(ModelStatusInfo.ModelStatus.LOADED, status(manageModels, "myModel2"));
            assertEquals(ModelStatusInfo.ModelStatus.LOADED, status.getStatus());

            long t2 = System.nanoTime();
            status = manageModels.registerModel(newModel.setModelId("myModel20").build());
            tookMillis = (System.nanoTime() - t2) / 1000_000L;
            // model2 should now be unloading
            assertEquals(ModelStatusInfo.ModelStatus.NOT_LOADED, status(manageModels, "myModel2"));
            assertEquals(ModelStatusInfo.ModelStatus.LOADED, status.getStatus());

            // This is the unloading time plus the 200ms pause that MM makes prior to unloading
            long expectedTime = 1000L + 200L - (t2 - t1) / 1000_000L;
            assertTrue(tookMillis > (expectedTime - 50)
                    && tookMillis < (expectedTime + 200), "took " + tookMillis + ", expected " + expectedTime);
            System.out.println("Load took " + tookMillis + "ms, expected " + expectedTime + "ms");

            // re-load model 0, which should evict myModel3
            status = manageModels.ensureLoaded(EnsureLoadedRequest.newBuilder().setModelId("myModel0").build());
            assertEquals(ModelStatusInfo.ModelStatus.LOADING, status.getStatus());

            // register a bigger model which should evict myModel4-6
            status = manageModels.registerModel(newModel.setModelId("myModel21_BIG").setModelInfo(ModelInfo.newBuilder()
                    .setPath("SIZE_"+(160*1024*1024)+"_20_10")
                    .setType("ExampleType").build()).build());
            assertEquals(ModelStatusInfo.ModelStatus.LOADED, status.getStatus());
            assertEquals(ModelStatusInfo.ModelStatus.NOT_LOADED, status(manageModels, "myModel3"));
            // Here we have to pause to allow sizing to complete since predicted size will still be 50MB even though
            // post-load reported size is 130
            Thread.sleep(150L);
            assertEquals(ModelStatusInfo.ModelStatus.NOT_LOADED, status(manageModels, "myModel4"));
            assertEquals(ModelStatusInfo.ModelStatus.NOT_LOADED, status(manageModels, "myModel5"));
            assertEquals(ModelStatusInfo.ModelStatus.NOT_LOADED, status(manageModels, "myModel6"));
            assertEquals(ModelStatusInfo.ModelStatus.LOADED, status(manageModels, "myModel7"));

        } finally {
            // clean up
            manageModels.unregisterModel(UnregisterModelRequest.newBuilder().setModelId("myModel21_BIG").build());
            for (int i = 0; i < 25; i++) {
                manageModels.unregisterModel(UnregisterModelRequest.newBuilder().setModelId("myModel" + i).build());
            }
            channel.shutdown();
        }
    }

    @Test
    public void concurrentEvictionTest() throws Exception {
        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 8088)
            .usePlaintext().build();
        ModelMeshGrpc.ModelMeshBlockingStub manageModels = ModelMeshGrpc.newBlockingStub(channel);
        try {

            // Template for the models that we'll register
            // They will take 20ms to load and 1 second to unload (opposite of "real" behaviour
            // but will allow us to test unload buffering)
            RegisterModelRequest.Builder newModel = RegisterModelRequest.newBuilder()
                .setModelInfo(ModelInfo.newBuilder()
                    .setPath("SIZE_"+(50*1024*1024)+"_20_1000")
                    .setType("ExampleType").build())
                .setLoadNow(true);

            // Add 17 models which should leave room for one more
            for (int i = 0; i < 17; i++) {
                manageModels.registerModel(newModel.setModelId("myModel" + i).build());
                Thread.sleep(10L);
            }
            // Add 18th model and wait for it to finish loading
            ModelStatusInfo status = manageModels.registerModel(newModel.setModelId("myModel17").setSync(true).build());
            assertEquals(ModelStatusInfo.ModelStatus.LOADED, status.getStatus());
            // Verify that all 18 report loaded
            for (int i = 0; i < 18; i++) {
                assertEquals(ModelStatusInfo.ModelStatus.LOADED, status(manageModels, "myModel" + i));
            }

            int threads = 10;
            ExecutorService es = Executors.newFixedThreadPool(threads);
            Phaser p = new Phaser(threads + 1);
            Future<ModelStatusInfo>[] futs = new Future[threads];
            for (int i = 0; i < threads; i++) {
                RegisterModelRequest req = newModel.setModelId("myModel" + (18 + i)).build();
                futs[i] = es.submit(() -> {
                    p.arriveAndAwaitAdvance();
                    return manageModels.registerModel(req);
                });
            }

            // Unleash the registration requests at exactly the same time.
            // With prior race conditions this would typically trigger an eviction cascade.
            p.arriveAndAwaitAdvance();
            for (int i = 0; i < threads; i++) {
                assertEquals(ModelStatusInfo.ModelStatus.LOADED, futs[i].get().getStatus());
            }
            es.shutdown();

            // First n models should be evicted
            for (int i = 0; i < threads; i++) {
                assertEquals(ModelStatusInfo.ModelStatus.NOT_LOADED, status(manageModels, "myModel" + i), ""+i);
            }

            // Remainder should all still be loaded
            for (int i = threads; i < 18; i++) {
                assertEquals(ModelStatusInfo.ModelStatus.LOADED, status(manageModels, "myModel" + i), ""+i);
            }

        } finally {
            // clean up
            for (int i = 0; i < 30; i++) {
                manageModels.unregisterModel(UnregisterModelRequest.newBuilder().setModelId("myModel" + i).build());
            }
            channel.shutdown();
        }
    }

    @Test
    public void concurrentEvictionAndUnloadTest() throws Exception {
        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 8088)
            .usePlaintext().build();
        ModelMeshGrpc.ModelMeshBlockingStub manageModels = ModelMeshGrpc.newBlockingStub(channel);
        try {

            // Template for the models that we'll register
            // They will take 20ms to load and 1 second to unload (opposite of "real" behaviour
            // but will allow us to test unload buffering)
            RegisterModelRequest.Builder newModel = RegisterModelRequest.newBuilder()
                .setModelInfo(ModelInfo.newBuilder()
                    .setPath("SIZE_"+(50*1024*1024)+"_20_1000")
                    .setType("ExampleType").build())
                .setLoadNow(true);

            // Add 17 models which should leave room for one more
            for (int i = 0; i < 17; i++) {
                manageModels.registerModel(newModel.setModelId("myModel" + i).build());
                Thread.sleep(10L);
            }
            // Add 18th model and wait for it to finish loading
            ModelStatusInfo status = manageModels.registerModel(newModel.setModelId("myModel17").setSync(true).build());
            assertEquals(ModelStatusInfo.ModelStatus.LOADED, status.getStatus());
            // Verify that all 18 report loaded
            for (int i = 0; i < 18; i++) {
                assertEquals(ModelStatusInfo.ModelStatus.LOADED, status(manageModels, "myModel" + i));
            }

            int regThreads = 10;
            int unregThreads = regThreads / 2;
            ExecutorService es = Executors.newFixedThreadPool(regThreads + unregThreads);
            Phaser p = new Phaser(regThreads + unregThreads + 1);
            Future<ModelStatusInfo>[] futs = new Future[regThreads];
            for (int i = 0; i < regThreads; i++) {
                RegisterModelRequest req = newModel.setModelId("myModel" + (18 + i)).build();
                futs[i] = es.submit(() -> {
                    p.arriveAndAwaitAdvance();
                    return manageModels.registerModel(req);
                });
            }

            // Also send requests to unload half of the to-be-evicted models, to test concurrent
            // eviction and explicit removal of the same models
            for (int i = 0; i < unregThreads; i++) {
                UnregisterModelRequest req = UnregisterModelRequest.newBuilder().setModelId("myModel" + i).build();
                es.submit(() -> {
                    p.arriveAndAwaitAdvance();
                    return manageModels.unregisterModel(req);
                });
            }

            // Unleash the registration requests at exactly the same time.
            // With prior race conditions this would typically trigger an eviction cascade.
            p.arriveAndAwaitAdvance();
            for (int i = 0; i < regThreads; i++) {
                assertEquals(ModelStatusInfo.ModelStatus.LOADED, futs[i].get().getStatus());
            }
            es.shutdown();

            // First n models should be evicted or not found
            for (int i = 0; i < regThreads; i++) {
                ModelStatusInfo.ModelStatus s = status(manageModels, "myModel" + i);
                assertTrue(ImmutableSet.of(ModelStatusInfo.ModelStatus.NOT_LOADED, ModelStatusInfo.ModelStatus.NOT_FOUND)
                    .contains(s), i+": "+s);
            }

            // Remainder should all still be loaded
            for (int i = regThreads; i < 18; i++) {
                assertEquals(ModelStatusInfo.ModelStatus.LOADED, status(manageModels, "myModel" + i), ""+i);
            }
        } finally {
            // clean up
            for (int i = 0; i < 30; i++) {
                manageModels.unregisterModel(UnregisterModelRequest.newBuilder().setModelId("myModel" + i).build());
            }
            channel.shutdown();
        }
    }

    // convenience method
    static ModelStatusInfo.ModelStatus status(ModelMeshGrpc.ModelMeshBlockingStub stub, String modelId) {
        return stub.getModelStatus(GetStatusRequest.newBuilder().setModelId(modelId).build()).getStatus();
    }
}
