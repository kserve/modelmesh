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
import com.ibm.watson.litelinks.client.LitelinksServiceClient;
import com.ibm.watson.litelinks.client.ThriftClientBuilder;
import com.ibm.watson.litelinks.server.LitelinksService;
import com.ibm.watson.modelmesh.thrift.LegacyModelMeshService;
import com.ibm.watson.modelmesh.thrift.ModelInfo;
import com.ibm.watson.modelmesh.thrift.Status;
import com.ibm.watson.zk.ZookeeperClient;
import org.apache.curator.test.TestingServer;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Train-and-serve runtime service unit test - ungraceful shutdown
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
public class ModelMeshZkFailTest {
    // Shared infrastructure
    private static TestingServer localZk;
    private static String localZkConnStr;
    private static File objstoreTemp;
    private static int serviceSerial;

    private static final String tasRuntimeClusterName = "tas-runtime-cluster-unit-test";

    private static final int clusterSize = 3;
    private static final List<Service> serviceCluster = new ArrayList<>(clusterSize);
    private static LegacyModelMeshService.Iface clusterClient;

    private static final String serviceType = "DUMMY";
    private static final String modelPath = "output/slad-3";
    protected static final int REGISTRY_BUCKETS = 128; // this shouldn't be changed


    @BeforeAll
    public static void initialize() throws Exception {
        // use dummy runtimes
        System.setProperty("tas.use_dummy_runtimes", "true");

        //shared infrastructure
        setupZookeeper();
        initializeObjectStore();

        //cluster mode
        createTasCluster();
    }

    @BeforeEach
    public void beforeEachTest() {
        System.out.println("[Client] ------------------   Starting New Test -------------------");
    }

    /**
     * For a TAS Cluster
     * Given
     * 1. Zookeeper has been setup AND
     * 2. Object store has been initialised AND
     * 3. Services have been created in separate processes AND
     * 4. Zookeeper Tables have been initialised
     * 5. Models have been exercised
     * When the Zookeeper server is shutdown,
     * Then model requests are still served
     */
    @Test
    public void basic_zk_restart_test() throws Exception {
        clusterClient = ThriftClientBuilder
                .newBuilder(LegacyModelMeshService.Iface.class)
                .withZookeeper(localZkConnStr + "/")
                .withServiceName(tasRuntimeClusterName)
                .withTimeout(8000)
                .buildOnceAvailable(8000);
        assertTrue(((LitelinksServiceClient) clusterClient).awaitAvailable(8000));
        System.out.println("Load models");
        List<String> modelIds = loadModels(6);
        System.out.println("first test");
        basic_test(modelIds);
        System.out.println("stopping zk");
        localZk.stop(); // kill the ZK server
        Thread.sleep(6000); // wait
        System.out.println("test after stopping zk");
        basic_test(modelIds);
        // wait 3 sec (past zk conn timeout)
        Thread.sleep(3000);
        // create new clusterClient
        System.out.println("test after waiting past conn timeout");
        basic_test(modelIds);
        // wait 4 more sec (now past zk session timeout)
        Thread.sleep(4000);
        System.out.println("test after waiting past session timeout");
        basic_test(modelIds);
        // now restart zk
        System.out.println("restarting zk");
        localZk.restart();
        Thread.sleep(2000);
        assertTrue(((LitelinksServiceClient) clusterClient).awaitAvailable(8000));
        basic_test(modelIds);
        // shutdown service
    }

    @AfterEach
    public void afterEachTest() {
        System.out.println("[Client] ------------------   Finished Test -------------------");
    }

    @AfterAll
    public static void shutdown() throws IOException {
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
    private List<String> loadModels(int num) throws TException, InterruptedException {
        List<String> modelIds = new ArrayList<>();
        ModelInfo modelInfo = new ModelInfo(serviceType, modelPath);
        for (int m = 0; m < num; m++) {
            String modelId = getNextModelId();
            assertEquals(Status.LOADED, clusterClient.addModel(modelId, modelInfo, true, true).getStatus());
            modelIds.add(modelId);
        }
        return modelIds;
    }

    private void basic_test(List<String> modelIds) throws InterruptedException, TimeoutException {
        LegacyModelMeshService.Iface client = ThriftClientBuilder
                .newBuilder(LegacyModelMeshService.Iface.class)
                .withZookeeper(localZkConnStr + "/")
                .withServiceName(tasRuntimeClusterName)
                .withTimeout(8000)
                .buildOnceAvailable(8000);
        for (int i = 0; i < 7; i++) {
            for (String modelId : modelIds) {
                try {
                    System.out.println(StandardCharsets.UTF_8.decode(client.applyModel(modelId,
                            ByteBuffer.wrap("foo".getBytes()), null)));
                } catch (Exception e) {
                    fail();
                }
            }
        }
    }

    private synchronized String getNextModelId() {
        return "nlc-" + ++serviceSerial;
    }

    private static void setupZookeeper() throws Exception {
        // Local ZK setup
        ZookeeperClient.SHORT_TIMEOUTS = true;
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

    private static void createTasCluster() throws InterruptedException, TimeoutException, IOException {
        // Create TAS cluster
        int replicaSetId = ThreadLocalRandom.current().nextInt(1 << 24);
        for (int i = 0; i < 3; i++) {
            System.setProperty(ModelMeshEnvVars.MMESH_METRICS_ENV_VAR, "prometheus:port=" + (2115 + i));
            Service svc =
                    LitelinksService.createService(new LitelinksService.ServiceDeploymentConfig(DummyModelMesh.class)
                            .setZkConnString(localZkConnStr).setServiceName(tasRuntimeClusterName)
                            .setServiceVersion("20170315-1347-2")
                            .setInstanceId(String.format("%06x-%05x", replicaSetId, i + 1)));
            svc.startAsync().awaitRunning();
            serviceCluster.add(svc);
            System.clearProperty(ModelMeshEnvVars.MMESH_METRICS_ENV_VAR);
        }
    }
}
