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
import com.ibm.watson.kvutils.JsonSerializer;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.ProcessBuilder.Redirect;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Train-and-serve runtime service unit test - ungraceful shutdown
 */
@Timeout(value = 50, unit = TimeUnit.SECONDS)
public class ModelMeshTearDownTest {
    // Shared infrastructure
    private static TestingServer localZk;
    private static String localZkConnStr;
    private static File objstoreTemp;
    private static int serviceSerial;

    private static final String tasRuntimeClusterName = "tas-runtime-cluster-unit-test";

    private static final int clusterSize = 3;
    private static final List<Process> serviceCluster = new ArrayList<>(clusterSize);
    private static LegacyModelMeshService.Iface clusterClient;
    private static ZookeeperKVTable clusterZkTable;
    private static TableView<ModelRecord> clusterRegistry;
    private static ZookeeperKVTable clusterInstanceTable;
    private static TableView<InstanceRecord> clusterInstanceInfo;

    private static final String serviceType = "DUMMY";
    private static final String modelPath = "output/slad-3";
    protected static final int REGISTRY_BUCKETS = 128; // this shouldn't be changed

    private final ModelMeshTearDownTest GIVEN = this;
    private final ModelMeshTearDownTest WHEN = this;
    private final ModelMeshTearDownTest THEN = this;
    private final ModelMeshTearDownTest AND = this;

    @BeforeAll
    public static void initialize() throws Exception {
        //shared infrastructure
        setupZookeeper();
        initializeObjectStore();

        //cluster mode
        createTasCluster();
        initializeTasClusterZookeeperTables();
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
     * When we ungracefully destroy a TAS process and perform a classify request on one of the violently evicted models,
     * Then the request is still served
     */
    @Test
    public void testDestroyNode() throws Exception {
        InstanceStateUtil.logModelRegistry(clusterRegistry);
        InstanceStateUtil.logInstanceInfo(clusterInstanceInfo);

        ModelInfo modelInfo = new ModelInfo(serviceType, modelPath);
        List<String> modelIds = new ArrayList<>();
        for (int i = 0; i < 3 * clusterSize; i++) {
            String modelId = getNextModelId();
            clusterClient.addModel(modelId, modelInfo, true, false);
            modelIds.add(modelId);
        }

        InstanceStateUtil.logModelRegistry(clusterRegistry);
        InstanceStateUtil.logInstanceInfo(clusterInstanceInfo);

        // Wait for all to be loaded, max of 20 seconds
        for (String modelId : modelIds) {
            assertEquals(Status.LOADED, clusterClient.addModel(modelId, modelInfo, true, true).getStatus());
        }

        System.out.println("Models loaded across " + clusterSize + " instances");

        InstanceStateUtil.logModelRegistry(clusterRegistry);
        InstanceStateUtil.logInstanceInfo(clusterInstanceInfo);

        // Tear down node
        System.out.println("Aboud to destroy one TAS instance process");
        Process p = serviceCluster.get(0);
        // p.destroy();
        killProcess(p);

        Thread.sleep(1200);
        InstanceStateUtil.logModelRegistry(clusterRegistry);
        InstanceStateUtil.logInstanceInfo(clusterInstanceInfo);

        // Exercise the client on the all IDs
        for (String modelId : modelIds) {
            clusterClient.applyModel(modelId, ByteBuffer.wrap("foo".getBytes()), null);
        }

        InstanceStateUtil.logModelRegistry(clusterRegistry);
        InstanceStateUtil.logInstanceInfo(clusterInstanceInfo);

        // Remove all added models
        for (String modelId : modelIds) {
            clusterClient.deleteModel(modelId);
        }

        InstanceStateUtil.logModelRegistry(clusterRegistry);
        InstanceStateUtil.logInstanceInfo(clusterInstanceInfo);
    }


    @AfterEach
    public void afterEachTest() {
        System.out.println("[Client] ------------------   Finished Test -------------------");
    }

    @AfterAll
    public static void shutdown() throws IOException {
        //Stop cluster
        for (Process p : serviceCluster) {
            if (p != null) {
                stopLitelinksProc(p);
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

    private static void createTasCluster() throws InterruptedException, TimeoutException, IOException {
        // Create TAS cluster, each in its own proc
        String[] jvmArgs = { "-Dtas.use_dummy_runtimes=true" };
        Map<String, String> envVars = new HashMap<>();
        envVars.put(ZookeeperClient.ZK_CONN_STRING_ENV_VAR, localZkConnStr);
        for (int i = 0; i < clusterSize; i++) {
            System.setProperty(ModelMeshEnvVars.MMESH_METRICS_ENV_VAR, "prometheus:port=" + (2115 + i));
            ProcessBuilder pb = buildTasSvcProcess(envVars, jvmArgs);
            Process svcProc = pb.start();
            serviceCluster.add(svcProc);
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

    public static ProcessBuilder buildTasSvcProcess(
            Map<String, String> envVars, String[] jvmArgs, String... testArgs) {
        List<String> args = new ArrayList<>(Arrays.asList(
                System.getProperty("java.home") + "/bin/java",
                "-cp", System.getProperty("java.class.path"),
                "-Xmx128M"));
        if (jvmArgs != null) {
            args.addAll(Arrays.asList(jvmArgs));
        }
        args.addAll(Arrays.asList(LitelinksService.class.getName(),
                "-s", DummyModelMesh.class.getName(), "-n", tasRuntimeClusterName));
        if (testArgs != null) {
            args.addAll(Arrays.asList(testArgs));
        }
        ProcessBuilder pb = new ProcessBuilder(args)
                .redirectErrorStream(true)
                .redirectOutput(Redirect.INHERIT);
        if (envVars != null) {
            pb.environment().putAll(envVars);
        }
        return pb;
    }

    private static void stopLitelinksProc(Process p) {
        try {
            PrintWriter pw = new PrintWriter(p.getOutputStream());
            pw.println("stop");
            pw.flush();
            for (int i = 0; i <= 16 && isRunning(p); i++) {
                Thread.sleep(500l);
                if (i == 16) {
                    fail("Service stop or deregistration timeout");
                }
            }
            if (p.exitValue() != 0) {
                System.err.println("Process exited with non-zero status code");
            }
        } catch (InterruptedException e) {
            System.err.println("Interrupted while waiting for process to stop");
        } finally {
            p.destroy(); // should already be gone in success cases
        }
    }

    public static boolean isRunning(Process proc) {
        try {
            proc.exitValue();
            return false;
        } catch (IllegalThreadStateException itse) {
            return true;
        }
    }

    public static int getPID(Process process) throws Exception {
        if (process.getClass().getName().equals("java.lang.UNIXProcess")) {
            Class clazz = process.getClass();
            Field field = clazz.getDeclaredField("pid");
            field.setAccessible(true);
            Object pidObject = field.get(process);
            return (Integer) pidObject;
        } else {
            return (int) process.pid();
        }
    }

    public static int killProcess(Process process) throws Exception {
        String pid = Integer.toString(getPID(process));
        String command = "kill -9";
        return Runtime.getRuntime().exec(new String[] {command, pid}).waitFor();
    }
}
