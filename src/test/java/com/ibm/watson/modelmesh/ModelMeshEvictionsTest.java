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
import com.ibm.watson.kvutils.JsonSerializer;
import com.ibm.watson.kvutils.KVTable;
import com.ibm.watson.kvutils.KVTable.TableView;
import com.ibm.watson.kvutils.factory.KVUtilsFactory;
import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import com.ibm.watson.litelinks.client.ThriftClientBuilder;
import com.ibm.watson.litelinks.server.LitelinksService;
import com.ibm.watson.modelmesh.thrift.LegacyModelMeshService;
import com.ibm.watson.modelmesh.thrift.ModelInfo;
import com.ibm.watson.modelmesh.thrift.Status;
import com.ibm.watson.modelmesh.util.InstanceStateUtil;
import com.ibm.watson.zk.ZookeeperClient;
import com.ibm.watson.zk.ZookeeperKVTable;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Train-and-serve runtime service unit tests - Capacity & Eviction Tests
 */
public class ModelMeshEvictionsTest {
    // Shared infrastructure
    private static TestingServer localZk;
    private static String localZkConnStr;
    private static File objstoreTemp;
    private static int serviceSerial;

    private static final String tasRuntimeStandaloneName = "tas-runtime-single-instance-unit-test";
    private static final String tasRuntimeClusterName = "tas-runtime-cluster-unit-test";

    private static final String serviceType = "DUMMY";
    private static final String modelPath = "output/slad-3";
    protected static final int REGISTRY_BUCKETS = 128; // this shouldn't be changed

    public static long DUMMY_CAPACITY;

    private final ModelMeshEvictionsTest GIVEN = this;
    private final ModelMeshEvictionsTest WHEN = this;
    private final ModelMeshEvictionsTest THEN = this;
    private final ModelMeshEvictionsTest AND = this;

    @BeforeAll
    public static void initialize() throws Exception {
        // Use dummies
        System.setProperty("tas.use_dummy_runtimes", "true");

        // otherwise requests will be rejected when we overflow the cache
        System.setProperty("tas.min_churn_age_ms", "1");

        DUMMY_CAPACITY = Long.getLong("tas.dummy_capacity",
                8 * DummyClassifierLoader.DEFAULT_MODEL_SIZE
                * ModelLoader.UNIT_SIZE);

        //shared infrastructure
        setupZookeeper();
        initializeObjectStore();

        //standalone mode
        createStandaloneTasService();
        initializeTasStandaloneZookeeperTables();

        System.setProperty("tas.janitor_freq_secs", "2");

        //cluster mode
        createTasCluster();
        initializeTasClusterZookeeperTables();
    }

    @BeforeEach
    public void beforeEachTest() {
        System.out.println("[Client] ------------------   Starting New Test -------------------");
    }

    // Standalone tests

    /**
     * For a Standalone TAS Instance
     * Given
     * 1. Zookeeper has been setup AND
     * 2. Object store has been initialised AND
     * 3. Services have been created AND
     * 4. Zookeeper Tables have been initialised
     * When we try to load multiple models but less than the total capacity then all models must be loaded
     */
    @Test
    public void testMultiLoadStandalone() throws Exception {
        System.out.println("[Client] testMultiLoadStandalone");

        //generate model Ids
        int modelLoadCount = 5;
        List<String> modelIds = generateModelIds(modelLoadCount);

        //load models
        testMultiLoad(standaloneClient, standaloneRegistry, standaloneInstanceInfo, modelIds);

        //verify state
        verifyMultiLoadState(standaloneClient, standaloneRegistry, standaloneInstanceInfo, modelIds);

        // destroy models after verification
        destroyModelsFromCluster(standaloneClient, standaloneRegistry, standaloneInstanceInfo, modelIds);
    }

    /**
     * For a Standalone TAS Instance
     * Given
     * 1. Zookeeper has been setup AND
     * 2. Object store has been initialised AND
     * 3. Services have been created AND
     * 4. Zookeeper Tables have been initialised
     * When we try to load multiple models exceeding the total capacity then the models loaded most recently must be present
     */
    @Test
    public void testMultiLoadWithEvictionStandalone() throws Exception {
        System.out.println("[Client] testMultiLoadWithEvictionStandalone");

        System.out.println("[Client] DUMMY_CAPACITY:" + DUMMY_CAPACITY
                           + " DummyClassifierLoader.DEFAULT_MODEL_SIZE:" + DummyClassifierLoader.DEFAULT_MODEL_SIZE
                           + " ModelLoader.UNIT_SIZE:" + ModelLoader.UNIT_SIZE);

        int maxModelsWithoutEviction =
                (int) (DUMMY_CAPACITY * 1 / (DummyClassifierLoader.DEFAULT_MODEL_SIZE * ModelLoader.UNIT_SIZE));
        System.out.println("[Client] No of models that can be loaded without eviction:" + maxModelsWithoutEviction);

        int modelsToLoadBeyondCapacity = 3;

        //generate model Ids
        int modelLoadCount = maxModelsWithoutEviction + modelsToLoadBeyondCapacity;
        List<String> generatedModelIds = generateModelIds(modelLoadCount);

        //get expected model ids for loaded models
        List<String> idsForModelsWhichShouldBeLoaded =
                getIdListOfModelsWhichShouldBeLoaded(generatedModelIds, modelLoadCount, maxModelsWithoutEviction, 0);

        //load models
        testMultiLoad(standaloneClient, standaloneRegistry, standaloneInstanceInfo, generatedModelIds);

        //verify state
        verifyMultiLoadState(standaloneClient, standaloneRegistry, standaloneInstanceInfo,
                idsForModelsWhichShouldBeLoaded);

        // destroy model records after verification
        destroyModelsFromCluster(standaloneClient, standaloneRegistry, standaloneInstanceInfo, generatedModelIds);
    }

    /**
     * For a Standalone TAS Instance Given
     * 1. Zookeeper has been setup AND
     * 2. Object store has been initialized AND
     * 3. Services have been created AND
     * 4. Zookeeper Tables have been initialized
     * When we try to load multiple models exceeding the total capacity
     * then the models loaded most recently must be present
     */
    @Test
    public void testMultiLoadWithEvictionStandaloneReuse() throws Exception {
        System.out.println("[Client] testMultiLoadWithEvictionStandaloneReuse");

        System.out.println("[Client] DUMMY_CAPACITY:" + DUMMY_CAPACITY
                           + " DummyClassifierLoader.DEFAULT_MODEL_SIZE:" + DummyClassifierLoader.DEFAULT_MODEL_SIZE
                           + " ModelLoader.UNIT_SIZE:" + ModelLoader.UNIT_SIZE);

        int maxModelsWithoutEviction = (int) (DUMMY_CAPACITY * 1
                                              / (DummyClassifierLoader.DEFAULT_MODEL_SIZE * ModelLoader.UNIT_SIZE));
        System.out.println("[Client] No of models that can be loaded without eviction:" + maxModelsWithoutEviction);

        int modelsToLoadBeyondCapacity = 3;

        // generate model Ids to fill instance and load them
        List<String> generatedModelIds = generateModelIds(maxModelsWithoutEviction);
        testMultiLoad(standaloneClient, standaloneRegistry, standaloneInstanceInfo, generatedModelIds);
        // verify state
        verifyMultiLoadState(standaloneClient, standaloneRegistry, standaloneInstanceInfo, generatedModelIds);

        // use the initial models to update their LRU time
        List<String> reusedModelIds = generatedModelIds.subList(0, 3);
        for (String modelId : reusedModelIds) {
            useModel(standaloneClient, modelId);
        }

        List<String> newModelIds = generateModelIds(modelsToLoadBeyondCapacity);
        testMultiLoad(standaloneClient, standaloneRegistry, standaloneInstanceInfo, newModelIds);
        verifyMultiLoadState(standaloneClient, standaloneRegistry, standaloneInstanceInfo, newModelIds);

        // get expected model ids for loaded models
        List<String> idsForModelsWhichShouldBeLoaded = (List<String>) ((ArrayList<String>) newModelIds).clone();
        idsForModelsWhichShouldBeLoaded.addAll(reusedModelIds);

        verifyMultiLoadState(standaloneClient, standaloneRegistry, standaloneInstanceInfo,
                idsForModelsWhichShouldBeLoaded);

        // destroy model records after verification
        destroyModelsFromCluster(standaloneClient, standaloneRegistry, standaloneInstanceInfo,
                idsForModelsWhichShouldBeLoaded);
    }

    // Cluster Tests

    /**
     * For a TAS Cluster
     * Given
     * 1. Zookeeper has been setup AND
     * 2. Object store has been initialised AND
     * 3. Services have been created AND
     * 4. Zookeeper Tables have been initialised
     * When we try to load multiple models but less than the total capacity then all models must be loaded
     */
    @Test
    public void testMultiLoadCluster() throws Exception {
        System.out.println("[Client] testMultiLoadCluster");

        //generate model Ids
        int modelLoadCount = 10;
        List<String> modelIds = generateModelIds(modelLoadCount);

        //load models
        testMultiLoad(clusterClient, clusterRegistry, clusterInstanceInfo, modelIds);

        // add this line if we want to check if the test fails on looking for a model which has not been loaded
        // modelIds.add(getNextModelId());

        //verify state
        verifyMultiLoadState(clusterClient, clusterRegistry, clusterInstanceInfo, modelIds);

        // destroy models after verification
        destroyModelsFromCluster(clusterClient, clusterRegistry, clusterInstanceInfo, modelIds);
    }

    /**
     * For a TAS Cluster
     * Given
     * 1. Zookeeper has been setup AND
     * 2. Object store has been initialised AND
     * 3. Services have been created AND
     * 4. Zookeeper Tables have been initialised
     * When we try to load multiple models exceeding the total capacity then the models loaded most recently must be present
     */
    @Test
    public void testMultiLoadWithEvictionCluster() throws Exception {
        System.out.println("[Client] testMultiLoadWithEvictionCluster");

        System.out.println("[Client] DUMMY_CAPACITY:" + DUMMY_CAPACITY
                           + " DummyClassifierLoader.DEFAULT_MODEL_SIZE:" + DummyClassifierLoader.DEFAULT_MODEL_SIZE
                           + " ModelLoader.UNIT_SIZE:" + ModelLoader.UNIT_SIZE);

        int maxModelsWithoutEviction = (int) (DUMMY_CAPACITY * clusterSize /
                                              (DummyClassifierLoader.DEFAULT_MODEL_SIZE * ModelLoader.UNIT_SIZE));
        System.out.println("[Client] No of models that can be loaded without eviction:" + maxModelsWithoutEviction);

        int modelsToLoadBeyondCapacity = 3;

        // wiggle room because there is intentionally some thresholds around instance
        // selection between instances which have "close" in terms of available space or LRU entry time
        int wiggleRoom = clusterSize * 2;

        //generate model Ids
        int modelLoadCount = maxModelsWithoutEviction + modelsToLoadBeyondCapacity;
        List<String> generatedModelIds = generateModelIds(modelLoadCount);

        //get expected model ids for loaded models
        List<String> idsForModelsWhichShouldBeLoaded = getIdListOfModelsWhichShouldBeLoaded(generatedModelIds,
                modelLoadCount, maxModelsWithoutEviction, wiggleRoom);

        //load models
        testMultiLoad(clusterClient, clusterRegistry, clusterInstanceInfo, generatedModelIds);

        //verify state
        verifyMultiLoadState(clusterClient, clusterRegistry, clusterInstanceInfo, idsForModelsWhichShouldBeLoaded);

        // destroy model records after verification
        destroyModelsFromCluster(clusterClient, clusterRegistry, clusterInstanceInfo, generatedModelIds);
    }

    /**
     * For a TAS Cluster Given
     * 1. Zookeeper has been setup AND
     * 2. Object store has been initialized AND
     * 3. Services have been created AND
     * 4. Zookeeper Tables have been initialized
     * When we try to load multiple models
     * exceeding the total capacity then the models loaded most recently must be
     * present
     */
    @Test
    public void testMultiLoadWithEvictionClusterReuse() throws Exception {
        System.out.println("[Client] testMultiLoadWithEvictionClusterReuse");

        System.out.println("[Client] DUMMY_CAPACITY:" + DUMMY_CAPACITY
                           + " DummyClassifierLoader.DEFAULT_MODEL_SIZE:" + DummyClassifierLoader.DEFAULT_MODEL_SIZE
                           + " ModelLoader.UNIT_SIZE:" + ModelLoader.UNIT_SIZE);

        int maxModelsWithoutEviction = (int) (DUMMY_CAPACITY * clusterSize
                                              / (DummyClassifierLoader.DEFAULT_MODEL_SIZE * ModelLoader.UNIT_SIZE));
        System.out.println("[Client] No of models that can be loaded without eviction:" + maxModelsWithoutEviction);

        int modelsToLoadBeyondCapacity = 3;

        // generate model Ids to fill instance and load them
        List<String> generatedModelIds = generateModelIds(maxModelsWithoutEviction);
        testMultiLoad(clusterClient, clusterRegistry, clusterInstanceInfo, generatedModelIds);
        // verify state
        verifyMultiLoadState(clusterClient, clusterRegistry, clusterInstanceInfo, generatedModelIds);

        // use the initial models to update their LRU time
        List<String> reusedModelIds = generatedModelIds.subList(0, 5);
        for (String modelId : reusedModelIds) {
            useModel(clusterClient, modelId);
        }

        List<String> newModelIds = generateModelIds(modelsToLoadBeyondCapacity);
        testMultiLoad(clusterClient, clusterRegistry, clusterInstanceInfo, newModelIds);
        verifyMultiLoadState(clusterClient, clusterRegistry, clusterInstanceInfo, newModelIds);

        // get expected model ids for loaded models
        List<String> idsForModelsWhichShouldBeLoaded = (List<String>) ((ArrayList<String>) newModelIds).clone();
        idsForModelsWhichShouldBeLoaded.addAll(reusedModelIds);

        verifyMultiLoadState(clusterClient, clusterRegistry, clusterInstanceInfo, idsForModelsWhichShouldBeLoaded);

        // destroy model records after verification
        destroyModelsFromCluster(clusterClient, clusterRegistry, clusterInstanceInfo, generatedModelIds);
    }

    @Test
    public void testMultiCopies() throws Exception {
        String modelId = "mymodel";
        ModelInfo modelInfo = new ModelInfo(serviceType, modelPath);
        assertEquals(Status.LOADED, clusterClient.addModel(modelId, modelInfo, true, true).getStatus());

        // only one copy gets loaded during the add
        assertEquals(1, clusterRegistry.get(modelId).getInstanceIds().size());

        clusterClient.applyModel(modelId, null, null);
        Set<String> instances = new HashSet<>();
        Thread.sleep(3000L);
        // this "usage" should trigger a second copy to be loaded
        clusterClient.applyModel(modelId, null, null);
        Thread.sleep(3000L);
        for (int i = 0; i < 8; i++) {
            instances.add(StandardCharsets.UTF_8.decode(
                    clusterClient.applyModel(modelId, null, null)).toString());
        }
        assertEquals(2, instances.size());
        assertEquals(2, clusterRegistry.get(modelId).getInstanceIds().size());
        clusterClient.deleteModel(modelId);
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
        //Stop cluster
        for (Service svc : serviceCluster) {
            if (svc != null) {
                svc.stopAsync().awaitTerminated();
            }
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
    private static ZookeeperKVTable standaloneZkTable;
    private static TableView<ModelRecord> standaloneRegistry;
    private static ZookeeperKVTable standaloneInstanceTable;
    private static TableView<InstanceRecord> standaloneInstanceInfo;

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

    private static void initializeTasStandaloneZookeeperTables() throws Exception {
        CuratorFramework cf = ZookeeperClient.getCurator(localZkConnStr, true);

        standaloneZkTable = new ZookeeperKVTable(cf,
                ZKPaths.makePath("/tas-runtime", tasRuntimeStandaloneName, "registry"),
                REGISTRY_BUCKETS);
        standaloneRegistry = standaloneZkTable.getView(new JsonSerializer<>(ModelRecord.class), 1);
        standaloneZkTable.start(3L, TimeUnit.MINUTES);
        standaloneInstanceTable = new ZookeeperKVTable(cf,
                ZKPaths.makePath("/tas-runtime", tasRuntimeStandaloneName, "instances"),
                0);
        standaloneInstanceInfo = standaloneInstanceTable.getView(new JsonSerializer<>(InstanceRecord.class), 1);
        standaloneInstanceTable.start(3L, TimeUnit.MINUTES);
    }

    private static final int clusterSize = 3;
    private static final List<Service> serviceCluster = new ArrayList<>(clusterSize);
    private static LegacyModelMeshService.Iface clusterClient;
    private static KVTable clusterZkTable;
    private static TableView<ModelRecord> clusterRegistry;
    private static KVTable clusterInstanceTable;
    private static TableView<InstanceRecord> clusterInstanceInfo;

    private static void createTasCluster() throws InterruptedException, TimeoutException {
        // Create TAS cluster
        for (int i = 0; i < clusterSize; i++) {
            System.setProperty(ModelMeshEnvVars.MMESH_METRICS_ENV_VAR, "prometheus:port=" + (2115 + i));
            Service svc =
                    LitelinksService.createService(new LitelinksService.ServiceDeploymentConfig(DummyModelMesh.class)
                            .setZkConnString(localZkConnStr).setServiceName(tasRuntimeClusterName)
                            .setServiceVersion("20170315-1347-2"));
            svc.startAsync().awaitRunning();
            serviceCluster.add(svc);
            System.clearProperty(ModelMeshEnvVars.MMESH_METRICS_ENV_VAR);
        }

        clusterClient = ThriftClientBuilder
                .newBuilder(LegacyModelMeshService.Iface.class)
                .withZookeeper(localZkConnStr)
                .withServiceName(tasRuntimeClusterName)
                .withTimeout(20000)
                .buildOnceAvailable(20000);
    }

    private static void initializeTasClusterZookeeperTables() throws Exception {
        CuratorFramework cf = ZookeeperClient.getCurator(localZkConnStr, true);

        clusterZkTable = new ZookeeperKVTable(cf,
                ZKPaths.makePath("/tas-runtime", tasRuntimeClusterName, "registry"),
                REGISTRY_BUCKETS);
        clusterRegistry = clusterZkTable.getView(new JsonSerializer<>(ModelRecord.class), 1);
        clusterZkTable.start(3L, TimeUnit.MINUTES);
        clusterInstanceTable = new ZookeeperKVTable(cf,
                ZKPaths.makePath("/tas-runtime", tasRuntimeClusterName, "instances"),
                0);
        clusterInstanceInfo = clusterInstanceTable.getView(new JsonSerializer<>(InstanceRecord.class), 1);
        clusterInstanceTable.start(3L, TimeUnit.MINUTES);
    }

    private boolean modelRecordInTasRegistry(String modelId, TableView<ModelRecord> registry) {
        try {
            ModelRecord mr = registry.get(modelId);
            if (mr != null) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean modelLoadedInTasRegistry(String modelId, TableView<ModelRecord> registry) {
        try {
            ModelRecord mr = registry.get(modelId);
            if (mr != null) {
                if (!mr.getInstanceIds().isEmpty()) {
                    return true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public void testMultiLoad(LegacyModelMeshService.Iface client, TableView<ModelRecord> registry,
            TableView<InstanceRecord> instanceInfo, List<String> modelIds) throws Exception {

        ModelInfo modelInfo = new ModelInfo(serviceType, modelPath);

        //log state before models added
        InstanceStateUtil.logModelRegistry(registry);
        InstanceStateUtil.logInstanceInfo(instanceInfo);

        //load models
        for (String modelId : modelIds) {
            client.addModel(modelId, modelInfo, true, false);
            boolean loaded = false;
            for (int i = 0; i < 20; i++) {
                if (loaded = Status.LOADED.equals(client.ensureLoaded(modelId, 0, null, false, true).getStatus())) {
                    break;
                } else {
                    Thread.sleep(1200);
                }
            }
            assertTrue(loaded);

            //log state after each model added
            InstanceStateUtil.logModelRegistry(registry);
            InstanceStateUtil.logInstanceInfo(instanceInfo);
        }
    }

    private void destroyModelsFromCluster(LegacyModelMeshService.Iface client,
            TableView<ModelRecord> registry,
            TableView<InstanceRecord> instanceInfo,
            List<String> modelIds) throws TException, InterruptedException {

        System.out.println("[Client] Delete Models");

        //Delete models
        for (String modelId : modelIds) {
            if (modelRecordInTasRegistry(modelId, registry)) {
                client.deleteModel(modelId);
            }
        }

        //wait
        Thread.sleep(1000);

        // Ensure deleted
        for (String modelId : modelIds) {
            assertFalse(modelRecordInTasRegistry(modelId, registry));
        }

        // log state
        InstanceStateUtil.logModelRegistry(registry);
        InstanceStateUtil.logInstanceInfo(instanceInfo);
    }

    private void verifyMultiLoadState(LegacyModelMeshService.Iface client,
            TableView<ModelRecord> registry,
            TableView<InstanceRecord> instanceInfo,
            List<String> modelIds) {

        System.out.println("[Client] Verify model loads");
        // log state
        InstanceStateUtil.logModelRegistry(registry);
        InstanceStateUtil.logInstanceInfo(instanceInfo);

        for (String modelId : modelIds) {
            if (!modelLoadedInTasRegistry(modelId, registry)) {
                System.out.println("[Client] ModelId not in registry " + modelId);
            }
            assertTrue(modelLoadedInTasRegistry(modelId, registry));
        }
    }

    private List<String> getIdListOfModelsWhichShouldBeLoaded(
            List<String> modelIds, int modelLoadCount,
            int maxModelsWithoutEviction, int wiggleRoom) {
        assertTrue(maxModelsWithoutEviction < modelLoadCount);
        int difference = modelLoadCount - maxModelsWithoutEviction;
        int startIndex = Math.min(modelIds.size() - 1, difference + wiggleRoom);
        return modelIds.subList(startIndex, modelIds.size());
    }

    private void useModel(LegacyModelMeshService.Iface client, String modelId) throws Exception {
        client.internalOperation(modelId, true, true, true, 0L, new ArrayList<String>());
    }
}
