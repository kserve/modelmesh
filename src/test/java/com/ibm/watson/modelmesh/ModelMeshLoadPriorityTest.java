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

import com.google.common.io.Files;
import com.google.common.util.concurrent.Service;
import com.ibm.watson.kvutils.factory.KVUtilsFactory;
import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import com.ibm.watson.litelinks.client.ThriftClientBuilder;
import com.ibm.watson.litelinks.server.LitelinksService;
import com.ibm.watson.modelmesh.thrift.LegacyModelMeshService;
import com.ibm.watson.modelmesh.thrift.ModelInfo;
import com.ibm.watson.modelmesh.thrift.Status;
import com.ibm.watson.zk.ZookeeperClient;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Train-and-serve runtime service unit tests - Loading prioritization tests
 */
public class ModelMeshLoadPriorityTest {
    // Shared infrastructure
    private static TestingServer localZk;
    private static String localZkConnStr;
    private static File objstoreTemp;
    private static int serviceSerial;

    private static final String tasRuntimeStandaloneName = "tas-runtime-single-instance-unit-test";

    private static final String serviceType = "DUMMY";
    private static final String modelPath = "output/slad-3";

    public static long DUMMY_CAPACITY;

    private final ModelMeshLoadPriorityTest GIVEN = this;
    private final ModelMeshLoadPriorityTest WHEN = this;
    private final ModelMeshLoadPriorityTest THEN = this;
    private final ModelMeshLoadPriorityTest AND = this;

    @BeforeAll
    public static void initialize() throws Exception {
        // Use dummies
        System.setProperty("tas.use_dummy_runtimes", "true");

        // otherwise requests will be rejected when we overflow the cache
        System.setProperty("tas.min_churn_age_ms", "1");

        // set loading pool to 1 to serialize loads
        System.setProperty("tas.loading_thread_count", "1");

        DUMMY_CAPACITY = Long.getLong("tas.dummy_capacity",
                8 * DummyClassifierLoader.DEFAULT_MODEL_SIZE
                * ModelLoader.UNIT_SIZE);

        //shared infrastructure
        setupZookeeper();
        initializeObjectStore();

        //standalone mode
        createStandaloneTasService();

        // reset property for other unit tests
        System.clearProperty("tas.loading_thread_count");
    }

    @BeforeEach
    public void beforeEachTest() {
        System.out.println("[Client] ------------------   Starting New Test -------------------");
    }

    // Standalone tests

    @Test
    public void testLoadingPriority() throws Exception {
        //generate model Ids
        ModelInfo modelInfo = new ModelInfo(serviceType, modelPath);
        List<String> modelIds = generateModelIds(3);
        // add models without loading
        for (String modelId : modelIds) {
            standaloneClient.addModel(modelId, modelInfo, false, false);
        }

        // ensureloaded two of them
        standaloneClient.ensureLoaded(modelIds.get(0), 0L, null, false, false);
        Thread.sleep(20L);
        standaloneClient.ensureLoaded(modelIds.get(1), 0L, null, false, false);
        Thread.sleep(20L);

        // hit another one
        standaloneClient.applyModel(modelIds.get(2), ByteBuffer.wrap("prioritize me!".getBytes()), null);

        // second ensureloaded shouldn't be loaded if the question overtook it
        assertNotEquals(Status.LOADED, standaloneClient.getStatus(modelIds.get(1)).getStatus());
        // other two should be loaded
        assertEquals(Status.LOADED, standaloneClient.getStatus(modelIds.get(2)).getStatus());
        assertEquals(Status.LOADED, standaloneClient.getStatus(modelIds.get(0)).getStatus());

        // wait for the other one to finish loading to ensure the queue is empty for other test(s)
        standaloneClient.ensureLoaded(modelIds.get(1), 0L, null, true, false);
    }

    @Test
    public void testLoadingPriorityUpgrade() throws Exception {
        //generate model Ids
        ModelInfo modelInfo = new ModelInfo(serviceType, modelPath);
        List<String> modelIds = generateModelIds(3);
        // add models without loading
        for (String modelId : modelIds) {
            standaloneClient.addModel(modelId, modelInfo, false, false);
        }

        // ensureloaded three of them
        standaloneClient.ensureLoaded(modelIds.get(0), 0L, null, false, false);
        Thread.sleep(20L);
        standaloneClient.ensureLoaded(modelIds.get(1), 0L, null, false, false);
        Thread.sleep(20L);
        standaloneClient.ensureLoaded(modelIds.get(2), 0L, null, false, false);
        Thread.sleep(20L);

        // hit the final one - it's 3rd in the queue but this should cause it to jump to 2nd
        standaloneClient.applyModel(modelIds.get(2), ByteBuffer.wrap("prioritize me!".getBytes()), null);

        // second ensureloaded shouldn't be loaded if the question model overtook it
        assertNotEquals(Status.LOADED, standaloneClient.getStatus(modelIds.get(1)).getStatus());
        // other two should be loaded
        assertEquals(Status.LOADED, standaloneClient.getStatus(modelIds.get(2)).getStatus());
        assertEquals(Status.LOADED, standaloneClient.getStatus(modelIds.get(0)).getStatus());

        // wait for the other one to finish loading to ensure the queue is empty for other test(s)
        standaloneClient.ensureLoaded(modelIds.get(1), 0L, null, true, false);
    }

    @AfterEach
    public void afterEachTest() {
        System.out.println("[Client] ------------------   Finished Test -------------------");
    }

    @AfterAll
    public static void shutdown() throws IOException {
        //Stop standalone
        if (standaloneService != null) {
            standaloneService.stopAsync().awaitTerminated();
        }
        //Shutdown zookeeper
        ZookeeperClient.shutdown(false, false);
        KVUtilsFactory.resetDefaultFactory();
        if (localZk != null) {
            localZk.close();
        }
    }

    // utility methods

    private synchronized String getNextModelId() {
        return "nlc-" + ++serviceSerial;
    }

    private synchronized List<String> generateModelIds(int modelLoadCount) {
        List<String> modelIds = new ArrayList<String>();
        for (int i = 0; i < modelLoadCount; i++) {
            modelIds.add(getNextModelId());
        }
        return modelIds;
    }

    private static void setupZookeeper() throws Exception {
        // Local ZK setup
        localZk = new TestingServer();
        localZk.start();
        localZkConnStr = localZk.getConnectString();
        System.setProperty(LitelinksSystemPropNames.SERVER_REGISTRY,
                "zookeeper:" + localZkConnStr);
        System.setProperty(KVUtilsFactory.KV_STORE_EV,
                "zookeeper:" + localZkConnStr);
    }

    private static void initializeObjectStore() {
        // Set up local object store
        objstoreTemp = Files.createTempDir();
        System.out.println("[Client] Object store path: " + objstoreTemp.getAbsolutePath());
        objstoreTemp.deleteOnExit();
        String objstore = "filesys,root=" + objstoreTemp.getAbsolutePath();
        System.setProperty("watson_objectstore", objstore);
    }

    private static Service standaloneService;
    private static LegacyModelMeshService.Iface standaloneClient;

    private static void createStandaloneTasService() throws InterruptedException, TimeoutException {
        // Create standalone TAS service
        Service service =
                LitelinksService.createService(new LitelinksService.ServiceDeploymentConfig(DummyModelMesh.class)
                        .setZkConnString(localZkConnStr).setServiceName(tasRuntimeStandaloneName)
                        .setServiceVersion("20170315-1347-2"));
        service.startAsync();
        standaloneService = service;

        //build clients
        standaloneClient = ThriftClientBuilder
                .newBuilder(LegacyModelMeshService.Iface.class)
                .withZookeeper(localZkConnStr)
                .withServiceName(tasRuntimeStandaloneName)
                .withTimeout(20000)
                .buildOnceAvailable(20000);
    }

}
