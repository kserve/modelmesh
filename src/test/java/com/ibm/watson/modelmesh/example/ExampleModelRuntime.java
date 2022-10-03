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

package com.ibm.watson.modelmesh.example;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ibm.watson.modelmesh.api.*;
import com.ibm.watson.modelmesh.api.RuntimeStatusResponse.MethodInfo;
import com.ibm.watson.modelmesh.api.RuntimeStatusResponse.Status;
import com.ibm.watson.modelmesh.example.api.ExamplePredictorGrpc.ExamplePredictorImplBase;
import com.ibm.watson.modelmesh.example.api.Predictor.CategoryAndConfidence;
import com.ibm.watson.modelmesh.example.api.Predictor.MultiPredictResponse;
import com.ibm.watson.modelmesh.example.api.Predictor.PredictRequest;
import com.ibm.watson.modelmesh.example.api.Predictor.PredictResponse;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Integer.parseInt;

/**
 * An example Model Serving runtime for use with the model-mesh framework
 */
public class ExampleModelRuntime extends ModelRuntimeGrpc.ModelRuntimeImplBase { // ModelServerImplBase {

    public static final boolean FAST_MODE = !"false".equals(System.getenv("FAST_MODE"));

    public static final String DEFAULT_PORT = "8085";

    private static final ThreadFactory DAEMON_TFAC = new ThreadFactoryBuilder().setDaemon(true).build();

    private static final LongAdder totalRequestCount = new LongAdder();

    // first command line arg can be either a port number, two comma-separated port numbers
    // (in which case the first port will be used for management and second for serving),
    // or of the form "unix://<socket-path>" where <socket-path> is a unix domain socket

    public static void main(String[] args) throws Exception {
        String listenOn = args.length > 0 ? args[0] : DEFAULT_PORT;
        int maxConc = args.length > 1 ? parseInt(args[1]) : 0;

        System.out.println("ENV: " + System.getenv());

        ExampleModelRuntime runtime = new ExampleModelRuntime(listenOn, maxConc);
        runtime.start().awaitTermination();
    }

    // ----------- startup

    protected final Server[] servers;

    protected final int maxConc;

    public ExampleModelRuntime(String listenOn, int maxConc) throws Exception {
        this.maxConc = maxConc;
        System.out.println("ExampleModelRuntime will listen on " + listenOn);
        String[] parts = listenOn.split(",");
        if (parts.length == 2) {
            servers = buildIpServers(parseInt(parts[0]), parseInt(parts[1]));
        } else {
            servers = new Server[] {
                listenOn.startsWith("unix://")
                    ? buildUdsServer(listenOn.substring(7))
                    : buildIpServer(parseInt(listenOn))
            };
        }
    }

    public ExampleModelRuntime(int port) throws Exception {
        this(port, 0);
    }

    // listen on port
    public ExampleModelRuntime(int port, int maxConc) {
        this.maxConc = maxConc;
        System.out.println("ExampleModelRuntime will listen on port " + port);
        servers = new Server[] { buildIpServer(port) };
    }

    private Server buildIpServer(int port) {
        return addPredictionService(addManagementService(serverBuilderForPort(port))).build();
    }

    private Server[] buildIpServers(int managementPort, int servingPort) {
        if (managementPort == servingPort) return new Server[] { buildIpServer(managementPort) };

        return new Server[] {
            addManagementService(serverBuilderForPort(managementPort)).build(),
            addPredictionService(serverBuilderForPort(servingPort)).build()
        };
    }

    private NettyServerBuilder serverBuilderForPort(int port) {
        NettyServerBuilder builder = NettyServerBuilder.forPort(port);
        if (Epoll.isAvailable()) {
            builder.channelType(EpollServerSocketChannel.class)
                    .bossEventLoopGroup(new EpollEventLoopGroup(1, DAEMON_TFAC))
                    .workerEventLoopGroup(new EpollEventLoopGroup(0, DAEMON_TFAC));
        }
        return builder;
    }

    private Server buildUdsServer(String socketPath) {
        new File(socketPath).getParentFile().mkdirs();
        NettyServerBuilder builder = NettyServerBuilder.forAddress(new DomainSocketAddress(socketPath))
                .channelType(EpollServerDomainSocketChannel.class)
                .bossEventLoopGroup(new EpollEventLoopGroup(1, DAEMON_TFAC))
                .workerEventLoopGroup(new EpollEventLoopGroup(0, DAEMON_TFAC));
        return addPredictionService(addManagementService(builder)).build();
    }

    private NettyServerBuilder addManagementService(NettyServerBuilder builder) {
        return builder.addService(this);
    }

    private NettyServerBuilder addPredictionService(NettyServerBuilder builder) {
        return builder.addService(ServerInterceptors.intercept(predictor, MODEL_ID_PASSER));
    }

    public ExampleModelRuntime start() throws Exception {
        System.out.println("ExampleModelRuntime starting");
        for (Server s : servers) s.start();
        Thread.sleep(FAST_MODE ? 1500L : 5_000L); // pretend startup - 5 seconds
        System.out.println("ExampleModelRuntime start complete");
        status = Status.READY;
        return this;
    }

    volatile Status status = Status.STARTING;


    /**
     * Provide startup status of this instance, and some local
     * parameters once ready
     */
    @Override
    public void runtimeStatus(RuntimeStatusRequest request,
            StreamObserver<RuntimeStatusResponse> response) {
        Status statusNow = status;

        RuntimeStatusResponse.Builder rsrBuilder = RuntimeStatusResponse
                .newBuilder().setStatus(statusNow);

        if (statusNow == Status.READY) {

            // as described in proto doc, all models should be purged here
            // (generally none should be loaded)
            if (!loadedModels.isEmpty()) {
                System.out.println("Unexpected runtimeStatus recieved; purging local cache");
                for (Iterator<Model> it = loadedModels.values().iterator(); it.hasNext(); ) {
                    it.next().unload();
                    it.remove();
                }
            }

            // dummy values
            rsrBuilder.setCapacityInBytes(1024 * 1024 * 1024); // 1GiB
            rsrBuilder.setDefaultModelSizeInBytes(50 * 1024 * 1024); // 50MiB
            rsrBuilder.setModelLoadingTimeoutMs(90_000); // 90secs
            rsrBuilder.setMaxLoadingConcurrency(6);

            rsrBuilder.setAllowAnyMethod("true".equals(System.getenv("RS_ALLOW_ANY")));
            String infos = System.getenv("RS_METHOD_INFOS");
            if (infos != null) {
                for (String entry : infos.split(";")) {
                    String[] parts = entry.split("=", -1);
                    MethodInfo.Builder minfo = MethodInfo.newBuilder();
                    if (!parts[1].isBlank()) {
                        for (String n : parts[1].split(",")) {
                            minfo.addIdInjectionPath(parseInt(n));
                        }
                    }
                    rsrBuilder.putMethodInfos(parts[0], minfo.build());
                }
            }
        }

        if (maxConc > 0) {
            rsrBuilder.setLimitModelConcurrency(true);
        }

        response.onNext(rsrBuilder.build());
        response.onCompleted();
    }

    public ExampleModelRuntime shutdown() {
        for (Server s : servers) s.shutdown();
        return this;
    }

    public void awaitTermination() throws InterruptedException {
        for (Server s : servers) s.awaitTermination();
    }


    // ----------- loading and unloading

    /**
     * Model id -> model mapping
     */
    private final Map<String, Model> loadedModels = new ConcurrentHashMap<>();

    /**
     * Load model from remote storage into memory
     */
    @Override
    public void loadModel(LoadModelRequest request, StreamObserver<LoadModelResponse> response) {

        String modelId = request.getModelId();
        Model newModel = new Model(modelId, request.getModelPath());

        String err = newModel.load();

        if (err != null) {
            response.onError(io.grpc.Status.INTERNAL
                    .withDescription(err).asException());
            return;
        }

        loadedModels.put(modelId, newModel);

        System.out.println("Loading model " + modelId + " complete");

        // if model size is already available or can be determined
        // very quickly here (i.e. 10s of millisecs at most), it can be
        // returned in the response per the commented line below.
        // If this is done, the modelSize() API method won't subsequently
        // be called, and need not be implemented.

        // LoadModelResponse lmr = LoadModelResponse.newBuilder().setSizeInBytes(modelSize).build();

        LoadModelResponse lmr = LoadModelResponse.getDefaultInstance();
        if (maxConc > 0) {
            lmr = LoadModelResponse.newBuilder().setMaxConcurrency(maxConc).build();
        }

        response.onNext(lmr);
        response.onCompleted();
    }

    /**
     * Unload already-loaded model from memory
     */
    @Override
    public void unloadModel(UnloadModelRequest request, StreamObserver<UnloadModelResponse> response) {

        String modelId = request.getModelId();
        Model model = loadedModels.remove(modelId);

        if (model != null) {
            model.unload();
        } else {
            // we didn't have the model anyhow - shouldn't happen but no problem
            if ("true".equals(System.getenv("UNLOAD_RETURN_NOT_FOUND"))) {
                System.out.println("Returning NOT_FOUND from unload of model " + modelId);
                response.onError(io.grpc.Status.NOT_FOUND.asRuntimeException());
                return;
            }
        }

        System.out.println("Unloading model " + modelId + " complete");

        response.onNext(UnloadModelResponse.getDefaultInstance());
        response.onCompleted();
    }

    // ------------ sizing

    /**
     * Implementing this method is optional but recommended if possible/fast
     *   - if not implemented will use defaultModelSizeInBytes
     */
//    @Override
//    public void predictModelSize(PredictModelSizeRequest request,
//            StreamObserver<PredictModelSizeResponse> responseObserver) {
//    }

    /**
     * For java-based model-serving runtimes, a very useful library for sizing
     * memory usage is "jamm" MemoryMeter - ask Nick about an optimized version
     */
    @Override
    public void modelSize(ModelSizeRequest request, StreamObserver<ModelSizeResponse> response) {

        Model model = loadedModels.get(request.getModelId());
        if (model == null) {
            // this should happen rarely if ever (e.g. if this container restarts unexpectedly)
            response.onError(io.grpc.Status.NOT_FOUND.asException());
            return;
        }

        response.onNext(ModelSizeResponse.newBuilder()
                .setSizeInBytes(model.calculateSizeBytes()).build());
        response.onCompleted();
    }

    // ------------- inferencing


    /*
     * The logic below shows how to serve models using a custom grpc
     * API definition (in this case src/main/proto/predictor.proto)
     *
     * It's added to the Server via this line in the main() method:
     *
     * .addService(ServerInterceptors.intercept(predictor, MODEL_ID_PASSER))
     *
     */

    static final Metadata.Key<String> MODEL_ID_META_KEY =
            Metadata.Key.of("mm-model-id", Metadata.ASCII_STRING_MARSHALLER);

    static final Context.Key<String> MODEL_ID_CTX_KEY =
            Context.key(MODEL_ID_META_KEY.name());

    /**
     * This interceptor must (unfortunately) be used in conjunction with the
     * custom service to allow it to access the model id passed via a Metadata header
     */
    static final ServerInterceptor MODEL_ID_PASSER = new ServerInterceptor() {
        @Override
        public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                Metadata headers, ServerCallHandler<ReqT, RespT> next) {
            //System.out.println("received headers: "+headers);
            String modelId = headers.get(MODEL_ID_META_KEY);
            return modelId == null ? next.startCall(call, headers)
                    : Contexts.interceptCall(Context.current().withValue(MODEL_ID_CTX_KEY,
                    modelId), call, headers, next);
        }
    };

    /**
     * This is your own service base, generated from src/main/proto/predictor.proto
     */
    final ExamplePredictorImplBase predictor = new ExamplePredictorImplBase() {
        @Override
        public void predict(PredictRequest request, StreamObserver<PredictResponse> response) {
            String modelId = MODEL_ID_CTX_KEY.get();

            PredictResponse result = doPredict(modelId, request, response);
            if (result == null) {
                return; // error already sent
            }

            response.onNext(result);
            response.onCompleted();
        }

        @Override
        public void multiPredict(PredictRequest request, StreamObserver<MultiPredictResponse> response) {
            String modelId = MODEL_ID_CTX_KEY.get();

            PredictResponse result = doPredict(modelId, request, response);
            if (result == null) {
                return; // error already sent
            }

            response.onNext(MultiPredictResponse.newBuilder()
                    .putPerModelResults(modelId, result).build());
            response.onCompleted();
        }

        private static final Pattern ERR_REQ_PATT = Pattern.compile("test:error:code=(\\w+):message=(.+)");

        private PredictResponse doPredict(String modelId,
                PredictRequest request, StreamObserver<?> response) {

            Model model = loadedModels.get(modelId);
            if (model == null) {
                // this should happen rarely if ever (e.g. if this container restarts unexpectedly)
                if ("true".equals(System.getenv("TRITON_NOT_FOUND_BEHAVIOUR"))) {
                    // Simulate Triton bug, see https://github.com/triton-inference-server/server/issues/3399
                    System.out.println("Responding with Triton-specific \"not found\" error (UNAVAILABLE code)");
                    response.onError(io.grpc.Status.UNAVAILABLE
                            .withDescription("Request for unknown model: '" + modelId + "' is not found")
                            .asRuntimeException());
                } else if ("true".equals(System.getenv("MLSERVER_NOT_FOUND_BEHAVIOUR"))) {
                    // Simulate MLServer bug
                    System.out.println("Responding with MLServer-specific \"not found\" error (INVALID_ARGUMENT code)");
                    response.onError(io.grpc.Status.INVALID_ARGUMENT
                        .withDescription("Model " + modelId + " with version  not found")
                        .asRuntimeException());
                } else {
                    response.onError(io.grpc.Status.NOT_FOUND.asException());
                }
                return null;
            }

            String stringToClassify = request.getText();

            performSpecialActions(stringToClassify);

            Matcher m = ERR_REQ_PATT.matcher(stringToClassify);
            if (m.matches()) {
                // Decode request intended to return a specific error
                response.onError(io.grpc.Status.fromCode(
                        io.grpc.Status.Code.valueOf(m.group(1)))
                        .withDescription(m.group(2)).asException());
                return null;
            }

            // perform the inferencing
            String classification = model.classify(stringToClassify);

            totalRequestCount.increment();
            // Set received model name as second result
            return PredictResponse.newBuilder().addResults(CategoryAndConfidence.newBuilder()
                            .setCategory(classification).setConfidence(1.0f))
                    .addResults(CategoryAndConfidence.newBuilder()
                            .setCategory(request.getModelName()))
                    .build();
        }
    };


    // for use in testing failure scenarios
    void performSpecialActions(String str) {
        if ("test:purge-models".equals(str)) {
            System.out.println("RECEIVED purge-models command - purging "
                               + loadedModels.size() + " loaded models");
            loadedModels.clear();
        }
//        else if("test:exit".equals(str)) {
//            System.out.println("RECEIVED exit command - exiting "
//                    + "runtime process with exit code 2");
//            System.out.flush();
//            System.exit(2);
//        }
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Example Runtime request count prior to shutdown: " + totalRequestCount.sum());
        }));
    }
}
