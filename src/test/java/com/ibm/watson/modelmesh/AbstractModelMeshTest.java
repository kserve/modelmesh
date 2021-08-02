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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Timeout;

import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Metadata;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;


/**
 * Model-mesh unit tests
 */
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public abstract class AbstractModelMeshTest {

    // returns kv store connection string
    protected String setupKvStore() throws Exception {
        return SetupEtcd.startEtcd();
    }

    protected void tearDownKvStore() throws Exception {
        SetupEtcd.stopEtcd();
    }

    static final Metadata.Key<String> MODEL_ID_META_KEY =
            Metadata.Key.of("mm-model-id", Metadata.ASCII_STRING_MARSHALLER);
    static final Metadata.Key<String> VMODEL_ID_META_KEY =
            Metadata.Key.of("mm-vmodel-id", Metadata.ASCII_STRING_MARSHALLER);

    static final Metadata.Key<String> CUST_HEADER_KEY =
            Metadata.Key.of("another-custom-header", Metadata.ASCII_STRING_MARSHALLER);

    public static <T extends AbstractStub<T>> T forModel(T stub, String... modelIds) {
        Metadata headers = new Metadata();
        for (String modelId : modelIds) {
            headers.put(MODEL_ID_META_KEY, modelId);
        }
        headers.put(CUST_HEADER_KEY, "custom-value");
        return MetadataUtils.attachHeaders(stub, headers);
    }

    public static <T extends AbstractStub<T>> T forVModel(T stub, String... vmodelIds) {
        Metadata headers = new Metadata();
        for (String modelId : vmodelIds) {
            headers.put(VMODEL_ID_META_KEY, modelId);
        }
        return MetadataUtils.attachHeaders(stub, headers);
    }

    // default KV store to test is etcd

    static Process etcdProcess;

    public static void setUpEtcdServer() throws Exception {
        boolean ok = false;
        try {
            etcdProcess = Runtime.getRuntime().exec("etcd --unsafe-no-fsync");
            waitForEtcdStartup();
            ok = true;
        } catch (IOException e) {
            System.out.println("Failed to start etcd: " + e);
            //e.printStackTrace();
        } finally {
            if (!ok) {
                tearDownEtcdServer();
            }
        }
    }

    static void tearDownEtcdServer() throws IOException {
        if (etcdProcess != null) {
            etcdProcess.destroy();
        }
    }

    static void waitForEtcdStartup() throws Exception {
        if (etcdProcess == null) {
            return;
        }
        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            SimpleTimeLimiter.create(es).callWithTimeout(() -> {
                Reader isr = new InputStreamReader(etcdProcess.getErrorStream());
                BufferedReader br = new BufferedReader(isr);
                String line;
                while ((line = br.readLine()) != null &&
                       !line.contains("ready to serve client requests")) {
                    System.out.println(line);
                }
                return null;
            }, 10L, TimeUnit.SECONDS);
        } finally {
            es.shutdown();
        }
    }

    protected Map<String, String> extraEnvVars() {
        return Collections.emptyMap();
    }

    protected Map<String, String> extraRuntimeEnvVars() {
        return Collections.emptyMap();
    }

    protected List<String> extraJvmArgs() {
        return Collections.emptyList();
    }

    protected List<String> extraLitelinksArgs() {
        return Collections.emptyList();
    }

    static class SetupEtcd {
        private static int refCount;
        private static ScheduledExecutorService exec = Executors
                .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).build());

        static synchronized String startEtcd() throws Exception {
            if (refCount == 0) {
                System.out.println("Starting etcd instance");
                AbstractModelMeshTest.setUpEtcdServer();
            }
            refCount++;
            return "etcd:http://localhost:2379";
        }

        static void stopEtcd() throws Exception {
            // Delay server shutdown so that it will be reused between tests
            exec.schedule(() -> {
                synchronized (SetupEtcd.class) {
                    if (refCount == 1) {
                        System.out.println("Stopping etcd instance");
                        try {
                            AbstractModelMeshTest.tearDownEtcdServer();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    refCount--;
                }
            }, 10, TimeUnit.SECONDS);
        }

        static {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    synchronized (SetupEtcd.class) {
                        if (refCount > 1) {
                            System.out.println("Calling shutdownhook for etcd instances...");
                            try {
                                AbstractModelMeshTest.tearDownEtcdServer();
                                refCount = 0;
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            });
        }
    }
}
