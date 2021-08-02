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
import com.ibm.watson.litelinks.server.LitelinksService.ServiceDeploymentConfig;
import com.ibm.watson.modelmesh.thrift.LegacyModelMeshService;
import com.ibm.watson.modelmesh.thrift.ModelInfo;
import com.ibm.watson.modelmesh.thrift.Status;
import com.ibm.watson.modelmesh.util.InstanceStateUtil;
import com.ibm.watson.zk.ZookeeperClient;
import com.ibm.watson.zk.ZookeeperKVTable;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Train-and-serve runtime service unit tests
 */
public class ModelMeshTest {
    // Shared infrastructure
    private static TestingServer localZk;
    private static String localZkConnStr;
    private static File objstoreTemp;
    private static int serviceSerial;
    private static final List<Service> services = new ArrayList<>(6);

    private static final String tasRuntimeStandaloneName = "tas-runtime-single-instance-unit-test";
    private static final String tasRuntimeClusterName = "tas-runtime-cluster-unit-test";

    private static final String serviceType = "DUMMY";
    private static final String modelPath = "output/slad-3";
    protected static final int REGISTRY_BUCKETS = 128; // this shouldn't be changed

    private final ModelMeshTest GIVEN = this;
    private final ModelMeshTest WHEN = this;
    private final ModelMeshTest THEN = this;
    private final ModelMeshTest AND = this;

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

    // Simple Standalone tests

    /**
     * For a Standalone TAS Instance
     * Given
     * 1. Zookeeper has been setup AND
     * 2. Object store has been initialised AND
     * 3. Services have been created AND
     * 4. Zookeeper Tables have been initialised
     * When we load and destroy a single model
     * Then there are no errors
     */
    @Test
    public void testLoadDestroyStandalone() throws Exception {
        System.out.println("[Client] testLoadDestroyStandalone");
        testLoadDestroy(standaloneClient, standaloneRegistry, standaloneInstanceInfo);
    }

    // Simple Cluster Tests

    /**
     * For a TAS Cluster
     * Given
     * 1. Zookeeper has been setup AND
     * 2. Object store has been initialised AND
     * 3. Services have been created AND
     * 4. Zookeeper Tables have been initialised
     * When we load and destroy a single model
     * Then there are no errors
     */
    @Test
    public void testLoadDestroyCluster() throws Exception {
        System.out.println("[Client] testLoadDestroyCluster");
        testLoadDestroy(clusterClient, clusterRegistry, clusterInstanceInfo);
    }

    @AfterEach
    public void afterEachTest() {
        System.out.println("[Client] ------------------   Finished Test -------------------");
    }

    @AfterAll
    public static void shutdown() throws IOException {
        for (Service svc : services) {
            if (svc != null) {
                svc.stopAsync().awaitTerminated();
            }
        }
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

    private static LegacyModelMeshService.Iface standaloneClient;
    private static KVTable standaloneZkTable;
    private static KVTable.TableView<ModelRecord> standaloneRegistry;
    private static KVTable standaloneInstanceTable;
    private static KVTable.TableView<InstanceRecord> standaloneInstanceInfo;

    private static void createStandaloneTasService() throws InterruptedException, TimeoutException {
        // Create standalone TAS service
        Service service = LitelinksService.createService(new ServiceDeploymentConfig(DummyModelMesh.class)
                .setZkConnString(localZkConnStr).setServiceName(tasRuntimeStandaloneName)
                .setServiceVersion("20170315-1347"));
        service.startAsync();
        services.add(service);

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

    private static LegacyModelMeshService.Iface clusterClient;
    private static KVTable clusterZkTable;
    private static KVTable.TableView<ModelRecord> clusterRegistry;
    private static KVTable clusterInstanceTable;
    private static KVTable.TableView<InstanceRecord> clusterInstanceInfo;

    private static void createTasCluster() throws InterruptedException, TimeoutException {
        // Create TAS cluster
        for (int i = 0; i < 5; i++) {
            System.setProperty(ModelMeshEnvVars.MMESH_METRICS_ENV_VAR, "prometheus:port=" + (2115 + i));
            Service svc = LitelinksService.createService(new ServiceDeploymentConfig(DummyModelMesh.class)
                    .setZkConnString(localZkConnStr).setServiceName(tasRuntimeClusterName)
                    .setServiceVersion("20170315-1347-2"));
            svc.startAsync().awaitRunning();
            services.add(svc);
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

    private boolean modelInTasRegistry(String modelId, TableView<ModelRecord> registry) {
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

    private void testLoadDestroy(LegacyModelMeshService.Iface client, TableView<ModelRecord> registry,
            TableView<InstanceRecord> instanceInfo) throws Exception {

        //log state before model added
        InstanceStateUtil.logModelRegistry(registry);
        InstanceStateUtil.logInstanceInfo(instanceInfo);

        String modelId = getNextModelId();
        ModelInfo modelInfo = new ModelInfo(serviceType, modelPath);
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
        //log state after model added
        InstanceStateUtil.logModelRegistry(registry);
        InstanceStateUtil.logInstanceInfo(instanceInfo);

        // Now make sure the ZK state looks correct
        assertTrue(modelInTasRegistry(modelId, registry));
        client.deleteModel(modelId);
        Thread.sleep(300);
        assertFalse(modelInTasRegistry(modelId, registry));

        //log state after model deleted
        InstanceStateUtil.logModelRegistry(registry);
        InstanceStateUtil.logInstanceInfo(instanceInfo);
    }

}
