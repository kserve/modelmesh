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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ProcessBuilder.Redirect;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import com.google.common.collect.ImmutableMap;
import com.ibm.watson.litelinks.server.LitelinksService;
import com.ibm.watson.modelmesh.example.ExampleModelRuntime;

/**
 * Model-mesh unit tests
 */
@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractModelMeshClusterTest extends AbstractModelMeshTest {

    private static final String clusterServiceName = "model-mesh-test-cluster";

    private static final Path tempDir;

    // Ensure we don't leave any child procs hanging around
    private static final List<Process> startedProcs
            = Collections.synchronizedList(new ArrayList<>());

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Process p : startedProcs) {
                if (p.isAlive()) {
                    p.destroyForcibly();
                }
            }
        }));
        try {
            tempDir = Files.createTempDirectory("mmesh-unittests");
            tempDir.toFile().deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    interface PodCloser extends Closeable {
        InputStream getInputStream();
    }

    protected abstract int replicaCount();

    protected boolean useDifferentInternalPortForInference() {
        return false;
    }

    private PodCloser[] podClosers;

    protected PodCloser[] getPodClosers() {
        return podClosers;
    }

    // Can be overridden
    protected Map<String, String> extraEnvVars(String replicaId) {
        return extraEnvVars();
    }

    @BeforeAll
    public void initialize() throws Exception {
        //shared infrastructure
        String kvStoreString = setupKvStore();
        Map<String, String> extraEnvVars = extraEnvVars();
        Map<String, String> extraRtEnvVars = extraRuntimeEnvVars();
        List<String> extraJvmArgs = extraJvmArgs();
        List<String> extraLlArgs = extraLitelinksArgs();

        String replicaSetId = "RS1";
        podClosers = new PodCloser[replicaCount()];
        for (int i = 0; i < podClosers.length; i++) {
            int port = 9000 + i * 4;
            String replicaId = Integer.toString(port);
            podClosers[i] = startModelMeshPod(kvStoreString, replicaSetId, port,
                    extraEnvVars(replicaId), extraRtEnvVars, extraJvmArgs, extraLlArgs,
                    useDifferentInternalPortForInference(), inheritIo());
        }
        System.out.println("started");
    }

    private static PodCloser startModelMeshPod(String kvStoreString,
            String replicaSetId, int port, Map<String, String> extraEnvVars,
            Map<String, String> extraRtEnvVars, List<String> extraJvmArgs,
            List<String> extraLlArgs, boolean diffInternalInferencePort, boolean inheritIo) throws Exception {
        int externalPort = port;
        int probePort = port + 1;
        int internalPort = port + 2;
        int internalServePort = diffInternalInferencePort ? port + 3 : internalPort;
        // use port num for replica id for now
        String replicaId = Integer.toString(port);
        ProcessBuilder mmPb = buildMmProcess(clusterServiceName,
                replicaSetId, replicaId,
                kvStoreString, externalPort, internalPort, internalServePort, probePort,
                extraEnvVars, extraJvmArgs, extraLlArgs, inheritIo);
        ProcessBuilder mrPb = buildExampleRuntimeProcess(
            internalPort + "," + internalServePort,  extraRtEnvVars);
        Process mmProc = mmPb.start(), mrProc = mrPb.start();
        startedProcs.add(mmProc);
        startedProcs.add(mrProc);
        String instId = clusterServiceName + "-" + replicaSetId + "-" + replicaId;
        //TODO have mechanism for controlled and force shutdown
        PodCloser stopper = new PodCloser() {
            @Override
            public InputStream getInputStream() {
                return mmProc.getInputStream();
            }
            @Override
            public void close() throws IOException {
                //TODO change this to poll for non-ready and then destroy runtime
                Optional<Duration> cpu = mmProc.info().totalCpuDuration();
                mmProc.onExit().whenComplete((p, t) -> {
                    String msg = getTerminationMessage(mmPb);
                    System.out.println("Termination message for instance " + instId + ": " + msg);
                    mrProc.destroy();
                });
                // Note this doesn't seem to work well on mac - the process is killed before
                // it has had time to shut down gracefully.
                mmProc.destroy();
                System.out.println("TOTAL CPU DURATION FOR PROC " + instId + ": " + cpu);
                try {
                    mrProc.waitFor();
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        };
        boolean ok = false;
        try {
            HttpClient hclient = HttpClient.newHttpClient();
            HttpRequest readyRequest = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + probePort + "/ready")).build();

            // Wait until ready
            String notReadyErr = null;
            for (int i = 0; i < 40; i++) {
                if (!mmProc.isAlive()) {
                    throw new Exception("ModelMesh process failed to start, exit code: " + mmProc.exitValue());
                }
                try {
                    int sc = hclient.send(readyRequest, BodyHandlers.discarding()).statusCode();
                    if (sc == 200) {
                        ok = true;
                        return stopper;
                    }
                    notReadyErr = "ModelMesh readiness probe returned " + sc;
                } catch (ConnectException ce) {
                    notReadyErr = String.valueOf(ce);
                }
                Thread.sleep(500);
            }
            throw new TimeoutException("ModelMesh process did not become ready within 20sec"
                    + (notReadyErr != null ? ": " + notReadyErr : ""));
        } finally {
            if (!ok) {
                stopper.close();
            }
        }
    }

    public static String getTerminationMessage(ProcessBuilder origBuilder) {
        try {
            File terminationMsgFile = new File(
                    origBuilder.environment().getOrDefault("LL_TERMINATION_MSG_PATH", ""));
            // Wait up to 5 sec for file to be properly written
            for (int i = 0; i < 10; i++) {
                if (terminationMsgFile.exists()) {
                    return Files.readString(terminationMsgFile.toPath());
                }
                Thread.sleep(500);
            }
            return "File " + terminationMsgFile + " does not exist";
        } catch (Exception e) {
            return "Error obtaining: " + e;
        }
    }

    protected boolean inheritIo() {
        return true; // can be overridden
    }

    //TODO could also test with -Dio.netty.transport.noNative=true

    public static ProcessBuilder buildMmProcess(String serviceName,
            String replicaSetId, String replicaId,
            String kvStore, int port, int internalPort, int internalServePort, int probePort,
            Map<String, String> extraEnvVars, List<String> extraJvmArgs, List<String> extraLLArgs,
            boolean inheritIo) {
        //TODO configure log4j for started proc to go to stdout
        String instId = serviceName + "-" + replicaSetId + "-" + replicaId;
        List<String> args = new ArrayList<>(Arrays.asList(
                System.getProperty("java.home") + "/bin/java",
                "-cp", System.getProperty("java.class.path"), "-Xmx128M",
                // Args set in real container
                "-Dfile.encoding=UTF8",
                "-Dio.netty.tryReflectionSetAccessible=true",
                "-Dio.grpc.netty.useCustomAllocator=false",
                "-Dlitelinks.produce_pooled_bytebufs=true",
                "-Dlitelinks.cancel_on_client_close=true",
                "-Dlitelinks.ssl.use_jdk=false",
                "-Dlitelinks.threadcontexts=log_mdc",
                // Paranoid bytebuf leak detection for tests
                "-Dio.netty.leakDetection.level=paranoid"));
        if (extraJvmArgs != null) {
            args.addAll(extraJvmArgs);
        }
        args.addAll(Arrays.asList(
                LitelinksService.class.getName(),
                "-s", SidecarModelMesh.class.getName(),
                "-n", serviceName, "-v", "20200101-0000",
                "-i", instId,
                "-h", "" + probePort));
        if (extraLLArgs != null) {
            args.addAll(extraLLArgs);
        }

        Map<String, String> env = ImmutableMap.<String, String>builder()
                .put("MM_SEND_DEST_ID", "false")
                .put("KV_STORE", kvStore)
                .put("INTERNAL_GRPC_PORT", "" + internalPort)
                .put("MM_SVC_GRPC_PORT", "" + port)
                // Set low scaleup threshold for now so that scaleup happens fast
                .put("MM_SCALEUP_RPM_THRESHOLD", "" + 10)
                .put("LL_TERMINATION_MSG_PATH", tempDir.resolve(instId).toString())
                .putAll(extraEnvVars)
                .putAll(internalPort == internalServePort ? Collections.emptyMap()
                    : Collections.singletonMap("INTERNAL_SERVING_GRPC_PORT", "" + internalServePort))
                .build();

        ProcessBuilder pb = new ProcessBuilder(args)
                .redirectErrorStream(true)
                //TODO the child procs' output doesn't go to the console
                // when running via maven surefire
                .redirectOutput(inheritIo ? Redirect.INHERIT : Redirect.PIPE);
        pb.environment().putAll(env);
        return pb;
    }

    public static ProcessBuilder buildExampleRuntimeProcess(String listenOn,
        Map<String, String> extraRtEnvVars) {
        //TODO configure log4j for started proc to go to stdout
        List<String> args = Arrays.asList(
                System.getProperty("java.home") + "/bin/java",
                "-cp", System.getProperty("java.class.path"), "-Xmx96M",
                ExampleModelRuntime.class.getName(), listenOn);
        ProcessBuilder pb = new ProcessBuilder(args)
                .redirectErrorStream(true)
                .redirectOutput(Redirect.INHERIT);
        pb.environment().put("FAST_MODE", "true");
        pb.environment().putAll(extraRtEnvVars);
        return pb;
    }

    @AfterAll
    public void shutdown() throws Exception {
        if (podClosers != null) {
            for (Closeable closer : podClosers) {
                if (closer != null) {
                    closer.close();
                }
            }
        }

        tearDownKvStore();
    }
}
