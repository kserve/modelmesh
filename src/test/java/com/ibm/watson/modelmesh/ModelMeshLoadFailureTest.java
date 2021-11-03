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
import com.ibm.watson.kvutils.KVTable.TableView;
import com.ibm.watson.kvutils.factory.KVUtilsFactory;
import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import com.ibm.watson.litelinks.client.LitelinksServiceClient;
import com.ibm.watson.litelinks.client.LitelinksServiceClient.ServiceInstanceInfo;
import com.ibm.watson.litelinks.client.ThriftClientBuilder;
import com.ibm.watson.litelinks.server.LitelinksService;
import com.ibm.watson.litelinks.server.WatchedService;
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
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Train-and-serve runtime service unit tests - Load Failure
 */
@Timeout(value = 25, unit = TimeUnit.SECONDS)
public class ModelMeshLoadFailureTest {
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

    private final ModelMeshLoadFailureTest GIVEN = this;
    private final ModelMeshLoadFailureTest WHEN = this;
    private final ModelMeshLoadFailureTest THEN = this;
    private final ModelMeshLoadFailureTest AND = this;

    @BeforeAll
    public static void initialize() throws Exception {
        // Use dummies
        System.setProperty("tas.use_dummy_runtimes", "true");

        //shared infrastructure
        setupZookeeper();
        initializeObjectStore();

        //standalone mode
        createStandaloneTasService();
        initializeTasStandaloneZookeeperTables();

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
     * When we try to load two models with the same model Id
     * Then this should return failure
     */
    @Test
    public void testLoadSameIdDifferentModelStandalone() throws Exception {
        System.out.println("[Client] testLoadSameIdDifferentModelStandalone");
        testLoadSameIdDifferentModel(standaloneClient, standaloneRegistry, standaloneInstanceInfo);
    }

    /**
     * For a Standalone TAS Instance
     * Given
     * 1. Zookeeper has been setup AND
     * 2. Object store has been initialised AND
     * 3. Services have been created AND
     * 4. Zookeeper Tables have been initialised
     * When we try to load a model and there is a failure that occurs (which in this case is forced)
     * Then this should result not in an exception but a LOADING_FAILED response
     */
    @Test
    public void testLoadFailureStandalone() throws Exception {
        System.out.println("[Client] testLoadFailureStandalone");
        testLoadFailure(standaloneClient, standaloneRegistry, standaloneInstanceInfo);
    }

    // Cluster Tests

    /**
     * For a TAS Cluster
     * Given
     * 1. Zookeeper has been setup AND
     * 2. Object store has been initialised AND
     * 3. Services have been created AND
     * 4. Zookeeper Tables have been initialised
     * When we try to load two models with the same model Id
     * Then this should return failure
     */
    @Test
    public void testLoadSameIdDifferentModelCluster() throws Exception {
        System.out.println("[Client] testLoadSameIdDifferentModelCluster");
        testLoadSameIdDifferentModel(clusterClient, clusterRegistry, clusterInstanceInfo);
    }

    /**
     * For a TAS Cluster
     * Given
     * 1. Zookeeper has been setup AND
     * 2. Object store has been initialised AND
     * 3. Services have been created AND
     * 4. Zookeeper Tables have been initialised
     * When we a load a model and failure occurs (which in this case it is forced)
     * Then the cluster should try to load the model on a different instance each time failure occurs
     * upto the point where MAX_LOAD_FAILURE has been reached and after that a LOADING_FAILED response must be received
     */
    @Test
    public void testLoadFailureCluster() throws Exception {
        System.out.println("[Client] testLoadFailureCluster");
        //define cluster size
        clusterSize = 5;
        resizeServiceCluster(clusterSize);

        testLoadFailure(clusterClient, clusterRegistry, clusterInstanceInfo);
    }

    /**
     * For a TAS Cluster
     * Given
     * 1. Zookeeper has been setup AND
     * 2. Object store has been initialised AND
     * 3. Services have been created AND
     * 4. Zookeeper Tables have been initialised
     * When we a load a model and failure might occur (since we have forcefully corrupted some instances)
     * Then the cluster should try to load the models on a different instance and if the the number of instances
     * on which it cannot load the model are less than the MAX_LOAD_FAILURE then all models must always gets loaded
     */
    @Test
    public void testLoadFailureRecovery() throws Exception {
        System.out.println("[Client] testLoadFailureRecovery");
        //define cluster size
        clusterSize = 3;
        resizeServiceCluster(clusterSize);

        //generate model Ids
        int modelLoadCount = 5;
        List<String> modelIds = generateModelIds(modelLoadCount);

        //load models
        testLoadFailureRecovery(clusterClient, clusterRegistry, clusterInstanceInfo, modelIds);

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
     * When a cluster with models loaded has a few services which need to be stopped
     * Then models on those instances get migrated to other instances
     */
    @Test
    public void testModelMigration() throws Exception {
        System.out.println("[Client] testModelMigration");
        //define minimum cluster size for testt
        if (clusterSize < 5) {
            clusterSize = 5;
        }
        resizeServiceCluster(clusterSize);

        //generate model Ids
        int modelLoadCount = 10;
        List<String> modelIds = generateModelIds(modelLoadCount);

        //load a few models
        testMultiLoad(clusterClient, clusterRegistry, clusterInstanceInfo, modelIds);

        //verify state
        verifyMultiLoadState(clusterClient, clusterRegistry, clusterInstanceInfo, modelIds);

        //ask shutdown of a service
        int numOfServicesToShutdown = 3;
        stopSomeServices(numOfServicesToShutdown);
        System.out.println("[Client] Some services were asked to shutdown");

        Thread.sleep(3000);

        //verify state
        verifyMultiLoadState(clusterClient, clusterRegistry, clusterInstanceInfo, modelIds);

        // destroy models after verification
        destroyModelsFromCluster(clusterClient, clusterRegistry, clusterInstanceInfo, modelIds);
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
    private static int prometheusPort = 2115;

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
        standaloneZkTable.start(2L, TimeUnit.MINUTES);
        standaloneInstanceTable = new ZookeeperKVTable(cf,
                ZKPaths.makePath("/tas-runtime", tasRuntimeStandaloneName, "instances"),
                0);
        standaloneInstanceInfo = standaloneInstanceTable.getView(new JsonSerializer<>(InstanceRecord.class), 1);
        standaloneInstanceTable.start(2L, TimeUnit.MINUTES);
    }

    private static int clusterSize = 5;
    private static final List<Service> serviceCluster = new ArrayList<Service>(clusterSize);
    private static LegacyModelMeshService.Iface clusterClient;
    private static ZookeeperKVTable clusterZkTable;
    private static TableView<ModelRecord> clusterRegistry;
    private static ZookeeperKVTable clusterInstanceTable;
    private static TableView<InstanceRecord> clusterInstanceInfo;

    private static final int replicaSetId = ThreadLocalRandom.current().nextInt(1 << 24);

    private static void createTasCluster() throws InterruptedException, TimeoutException {
        // Create TAS cluster
        for (int i = 0; i < clusterSize; i++) {
            System.setProperty(ModelMeshEnvVars.MMESH_METRICS_ENV_VAR, "prometheus:port=" + prometheusPort++);
            Service svc =
                    LitelinksService.createService(new LitelinksService.ServiceDeploymentConfig(DummyModelMesh.class)
                            .setZkConnString(localZkConnStr).setServiceName(tasRuntimeClusterName)
                            .setServiceVersion("20170315-1347-2")
                            .setInstanceId(String.format("%06x-%05x", replicaSetId, i + 1)));
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
        clusterZkTable.start(2L, TimeUnit.MINUTES);
        clusterInstanceTable = new ZookeeperKVTable(cf,
                ZKPaths.makePath("/tas-runtime", tasRuntimeClusterName, "instances"),
                0);
        clusterInstanceInfo = clusterInstanceTable.getView(new JsonSerializer<>(InstanceRecord.class), 1);
        clusterInstanceTable.start(2L, TimeUnit.MINUTES);
    }

    private void testLoadSameIdDifferentModel(LegacyModelMeshService.Iface client, TableView<ModelRecord> registry,
            TableView<InstanceRecord> instanceInfo) throws Exception {

        //log state before
        InstanceStateUtil.logModelRegistry(registry);
        InstanceStateUtil.logInstanceInfo(instanceInfo);

        String modelId = getNextModelId();
        ModelInfo modelInfoA = new ModelInfo(serviceType, modelPath);
        ModelInfo modelInfoB = new ModelInfo(serviceType, modelPath);

        modelInfoA.setEncKey("foo");
        modelInfoB.setEncKey("bar");

        System.out.println("[Client] Adding 1st model");
        client.addModel(modelId, modelInfoA, true, false);

        //state after 1st model addition was tried
        InstanceStateUtil.logModelRegistry(registry);
        InstanceStateUtil.logInstanceInfo(instanceInfo);

        try {
            System.out.println("[Client] Adding 2nd model");
            client.addModel(modelId, modelInfoB, true, false);
            fail("[Client] TException should have been thrown");
        } catch (TException e) {
        }

        //state after 2nd model addition was tried
        InstanceStateUtil.logModelRegistry(registry);
        InstanceStateUtil.logInstanceInfo(instanceInfo);

        // Now clean up
        client.deleteModel(modelId);
        Thread.sleep(200);
        assertFalse(modelRecordInTasRegistry(modelId, registry));
    }

    public void testLoadFailure(LegacyModelMeshService.Iface client, TableView<ModelRecord> registry,
            TableView<InstanceRecord> instanceInfo) throws Exception {

        //log state before calls
        InstanceStateUtil.logModelRegistry(registry);
        InstanceStateUtil.logInstanceInfo(instanceInfo);

        // Wait for the siiList to be same as cluster size
        // This is so that instances added during cluster resizing are all visible to us
        List<ServiceInstanceInfo> siiList = ((LitelinksServiceClient) client).getServiceInstanceInfo();
        for (int i = 0; i < 20; i++) {
            if (siiList.size() >= clusterSize) {
                break;
            } else {
                Thread.sleep(400);
                siiList = ((LitelinksServiceClient) client).getServiceInstanceInfo();
            }
        }
        assertNotNull(siiList);

        for (int i = 0; i < siiList.size(); i++) {
            DummyClassifierLoader.updateLoaderInstanceConfig(siiList.get(i).getInstanceId(),
                    DummyClassifierLoader.FORCE_FAIL_KEY, "true");
        }

        String modelId = getNextModelId();
        ModelInfo modelInfo = new ModelInfo(serviceType, modelPath);
        Status status = null;
        try {
            status = client.addModel(modelId, modelInfo, true, false).getStatus();

            //log state after add model call
            InstanceStateUtil.logModelRegistry(registry);
            InstanceStateUtil.logInstanceInfo(instanceInfo);

            for (int i = 0; status != Status.LOADING_FAILED && i < 20; i++) {
                System.out.println(status = client.ensureLoaded(modelId, 0, null, false, true).getStatus());
                Thread.sleep(1200);
            }
        } catch (TException e) {
            e.printStackTrace();
            fail("[Client] Should not throw exception, should only return LOADING_FAILED");
        }
        assertEquals(Status.LOADING_FAILED, status);

        //log state after unsuccessful attempt
        InstanceStateUtil.logModelRegistry(registry);
        InstanceStateUtil.logInstanceInfo(instanceInfo);

        //Ensure that the number of times the load was unsuccessfully tried was as expected
        ModelRecord mr = registry.get(modelId);
        if (siiList.size() > ModelMesh.MAX_LOAD_FAILURES) {
            int size = mr.getLoadFailedInstanceIds().size();
            assertTrue(size == ModelMesh.MAX_LOAD_FAILURES || size == ModelMesh.MAX_LOAD_FAILURES + 1);
        } else if (siiList.size() <= ModelMesh.MAX_LOAD_FAILURES) {
            assertEquals(siiList.size(), mr.getLoadFailedInstanceIds().size());
        }

        // Now clean up
        client.deleteModel(modelId);
        Thread.sleep(400);
        assertFalse(modelRecordInTasRegistry(modelId, registry));
    }

    public void testLoadFailureRecovery(LegacyModelMeshService.Iface client, TableView<ModelRecord> registry,
            TableView<InstanceRecord> instanceInfo, List<String> modelIds) throws Exception {

        ModelInfo modelInfo = new ModelInfo(serviceType, modelPath);

        //log state before models added
        InstanceStateUtil.logModelRegistry(registry);
        InstanceStateUtil.logInstanceInfo(instanceInfo);

        List<ServiceInstanceInfo> siiList = ((LitelinksServiceClient) client).getServiceInstanceInfo();
        assertNotNull(siiList);

        //Keep 1 instance which can load models i.e siiList.get(0)
        DummyClassifierLoader.updateLoaderInstanceConfig(siiList.get(0).getInstanceId(),
                DummyClassifierLoader.FORCE_FAIL_KEY, "false");
        //Other instances are forced to fail
        for (int i = 1; i < siiList.size(); i++) {
            DummyClassifierLoader.updateLoaderInstanceConfig(siiList.get(i).getInstanceId(),
                    DummyClassifierLoader.FORCE_FAIL_KEY, "true");
        }

        //load models
        for (String modelId : modelIds) {
            assertEquals(Status.LOADED, client.addModel(modelId, modelInfo, true, true).getStatus());

            //log state after each model added
            InstanceStateUtil.logModelRegistry(registry);
            InstanceStateUtil.logInstanceInfo(instanceInfo);
        }
    }

    public void testMultiLoad(LegacyModelMeshService.Iface client, TableView<ModelRecord> registry,
            TableView<InstanceRecord> instanceInfo, List<String> modelIds) throws Exception {

        ModelInfo modelInfo = new ModelInfo(serviceType, modelPath);

        //log state before models added
        InstanceStateUtil.logModelRegistry(registry);
        InstanceStateUtil.logInstanceInfo(instanceInfo);

        //Make all instances load-able again
        List<ServiceInstanceInfo> siiList = ((LitelinksServiceClient) client).getServiceInstanceInfo();
        assertNotNull(siiList);
        for (int i = 1; i < siiList.size(); i++) {
            DummyClassifierLoader.updateLoaderInstanceConfig(siiList.get(i).getInstanceId(),
                    DummyClassifierLoader.FORCE_FAIL_KEY, "false");
        }

        //load models
        for (String modelId : modelIds) {
            assertEquals(Status.LOADED, client.addModel(modelId, modelInfo, true, true).getStatus());

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
        Thread.sleep(200);

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

    private static void resizeServiceCluster(int newClusterSize) throws InterruptedException {

        //in case of expansion
        while (serviceCluster.size() < newClusterSize) {
            System.setProperty(ModelMeshEnvVars.MMESH_METRICS_ENV_VAR, "prometheus:port=" + prometheusPort++);
            System.out.println("[Client] serviceCluster(" + serviceCluster.size() + ") < than cluster size provided(" +
                               newClusterSize + ")  --> Resizing..");
            Service svc =
                    LitelinksService.createService(new LitelinksService.ServiceDeploymentConfig(DummyModelMesh.class)
                            .setZkConnString(localZkConnStr).setServiceName(tasRuntimeClusterName)
                            .setServiceVersion("20170315-1347-2")
                            .setInstanceId(String.format("%06x-%05x", replicaSetId, serviceCluster.size() + 1)));
            svc.startAsync().awaitRunning();
            serviceCluster.add(svc);
            System.clearProperty(ModelMeshEnvVars.MMESH_METRICS_ENV_VAR);
        }

        //in case of contraction
        if (serviceCluster.size() > newClusterSize) {
            System.out.println("[Client] serviceCluster(" + serviceCluster.size() + ") > than cluster size provided(" +
                               newClusterSize + ")  --> Resizing..");
            int numOfServicesToStop = serviceCluster.size() - newClusterSize;
            int servicesStopped = 0;
            for (ListIterator<Service> iterator = serviceCluster.listIterator(serviceCluster.size()); iterator.hasPrevious(); ) {
                if (servicesStopped >= numOfServicesToStop) {
                    break;
                }
                Service service = iterator.previous();
                service.stopAsync().awaitTerminated();
                servicesStopped++;
                iterator.remove();
            }
        }

        System.out.println("[Client] Final ClusterSize:" + clusterSize);
        System.out.println("[Client] Final ClusterServices:" + serviceCluster.stream().map(
            s -> ((WatchedService) s).getInstanceId()).collect(Collectors.joining(",")));
    }

    private synchronized List<String> generateModelIds(int modelLoadCount) {
        List<String> modelIds = new ArrayList<String>();
        for (int i = 0; i < modelLoadCount; i++) {
            modelIds.add(getNextModelId());
        }
        return modelIds;
    }

    private void stopSomeServices(int numOfServicesToStop) {
        int servicesStopped = 0;
        for (ListIterator<Service> iterator = serviceCluster.listIterator(serviceCluster.size()); iterator.hasPrevious(); ) {
            if (servicesStopped >= numOfServicesToStop) {
                break;
            }
            Service service = iterator.previous();
            service.stopAsync().awaitTerminated();
            servicesStopped++;
            iterator.remove();
        }
    }
}
