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

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.SettableFuture;
import com.ibm.etcd.client.FutureListener;
import com.ibm.watson.litelinks.NettyCommon;
import com.ibm.watson.litelinks.ThreadContext;
import com.ibm.watson.litelinks.server.Idempotent;
import com.ibm.watson.litelinks.server.ReleaseAfterResponse;
import com.ibm.watson.modelmesh.ModelMeshApi.ModelResponse;
import com.ibm.watson.modelmesh.api.LoadModelRequest;
import com.ibm.watson.modelmesh.api.LoadModelResponse;
import com.ibm.watson.modelmesh.api.ModelRuntimeGrpc;
import com.ibm.watson.modelmesh.api.ModelRuntimeGrpc.ModelRuntimeBlockingStub;
import com.ibm.watson.modelmesh.api.ModelRuntimeGrpc.ModelRuntimeFutureStub;
import com.ibm.watson.modelmesh.api.ModelSizeRequest;
import com.ibm.watson.modelmesh.api.ModelSizeResponse;
import com.ibm.watson.modelmesh.api.PredictModelSizeRequest;
import com.ibm.watson.modelmesh.api.PredictModelSizeResponse;
import com.ibm.watson.modelmesh.api.RuntimeStatusRequest;
import com.ibm.watson.modelmesh.api.RuntimeStatusResponse;
import com.ibm.watson.modelmesh.api.RuntimeStatusResponse.MethodInfo;
import com.ibm.watson.modelmesh.api.UnloadModelRequest;
import com.ibm.watson.modelmesh.api.UnloadModelResponse;
import com.ibm.watson.modelmesh.thrift.ApplierException;
import com.ibm.watson.modelmesh.thrift.BaseModelMeshService;
import com.ibm.watson.modelmesh.thrift.ModelInfo;
import com.ibm.watson.modelmesh.thrift.ModelMeshService;
import com.ibm.watson.modelmesh.thrift.ModelMeshService.Iface;
import com.ibm.watson.modelmesh.thrift.ModelNotFoundException;
import com.ibm.watson.modelmesh.thrift.ModelNotHereException;
import com.ibm.watson.tas.internal.proto.ModelServerGrpc;
import com.ibm.watson.tas.internal.proto.ModelServerGrpc.ModelServerBlockingStub;
import com.ibm.watson.tas.internal.proto.ModelServerGrpc.ModelServerFutureStub;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.InternalNettyChannelBuilder;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import javax.annotation.concurrent.GuardedBy;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.ibm.watson.modelmesh.GrpcSupport.asLeanException;
import static java.util.concurrent.TimeUnit.*;

/**
 * ModelMesh implementation for acting as a side-car to external but co-located
 * model service process, coupled via the internal ModelRuntime gRPC service interface.
 */
public final class SidecarModelMesh extends ModelMesh implements Iface {

    private static final Logger logger = LogManager.getLogger(SidecarModelMesh.class);

    public static final String GRPC_PORT_ENV_VAR = "INTERNAL_GRPC_PORT";
    public static final String SERVE_GRPC_PORT_ENV_VAR = "INTERNAL_SERVING_GRPC_PORT";

    public static final String GRPC_UDS_PATH_ENV_VAR = "INTERNAL_GRPC_SOCKET_PATH";
    public static final String SERVE_GRPC_UDS_PATH_ENV_VAR = "INTERNAL_SERVING_GRPC_SOCKET_PATH";

    public static final String DEFAULT_UDS_PATH = "/tmp/mmesh/grpc.sock";
    public static final int DEFAULT_GRPC_PORT = 8085;

    public static final int MODEL_SERVICE_STARTUP_TIMEOUT_SECS = 180;

    public static final long PREDICT_SIZE_TIMEOUT_MS = 500;
    public static final long SIZING_TIMEOUT_SECS = 30;

    // Number of times to retry model unloading failures before considering
    // fatal, at which point the associated capacity is considered "lost"
    static final int UNLOAD_RETRIES = 90; // equates to ~ 15 minutes
    final RateLimiter fullUnloadRetryRateLimiter = RateLimiter.create(0.1);  // allow 1 full unload retry every 10s

    protected static final String TAS_GRPC_METH_CXT_KEY = "tas.grpc.method";

    static final CallOptions DIRECT_EXEC_CALL_OPTS = CallOptions.DEFAULT.withExecutor(directExecutor());

    private static final ListenableFuture<UnloadModelResponse> UNLOAD_COMPLETE =
        Futures.immediateFuture(UnloadModelResponse.getDefaultInstance());

    private static final Joiner COMMA_JOIN = Joiner.on(',');
    private static final int[] EMPTY_INT_ARRAY = new int[0];

    private static final String MGMT_SERVICE_PREFIX = ModelRuntimeGrpc.SERVICE_NAME + '/';

    private /*final*/ ExternalModelLoader modelLoader;

    public SidecarModelMesh() throws Exception {
        super();
    }

    @Override
    protected int getRequestTimeoutMillis() {
        // no fixed timeout; set per-request based on grpc deadline
        return 0;
    }

    @Override
    protected LocalInstanceParameters startup() throws Exception {
        int maxGrpcMessageSize = getMaxGrpcMessageSize();
        logger.info("Maximum grpc response message size set to " + maxGrpcMessageSize + " bytes");

        ManagedChannel servingChannel = null;
        String servingPort = getStringParameter(SERVE_GRPC_PORT_ENV_VAR, null);
        String servingUdsPath = getStringParameter(SERVE_GRPC_UDS_PATH_ENV_VAR, null);
        if (servingPort != null) {
            if (servingUdsPath != null) {
                throw new Exception("Must provide at most one of " + SERVE_GRPC_PORT_ENV_VAR + " and "
                                    + SERVE_GRPC_UDS_PATH_ENV_VAR);
            }
            int sport = Integer.parseInt(servingPort);
            servingChannel = buildLocalChannel(sport, null, maxGrpcMessageSize);
            // We already have our serving channel so any other new channels won't
            // require the custom max message size
            maxGrpcMessageSize = 0;
            logger.info("Using grpc port " + sport + " for model serving "
                        + "traffic to collocated ModelServer container");
        } else if (servingUdsPath != null) {
            servingChannel = buildLocalChannel(-1, servingUdsPath, maxGrpcMessageSize);
            // We already have our serving channel so any other new channels won't
            // require the custom max message size
            maxGrpcMessageSize = 0;
            logger.info("Using grpc domain socket path " + servingUdsPath + " for model serving "
                        + "traffic to collocated ModelServer container");
        }

        List<ManagedChannel> candidates;
        String grpcPort = getStringParameter(GRPC_PORT_ENV_VAR, null);
        String udsPath = getStringParameter(GRPC_UDS_PATH_ENV_VAR, null);
        if (udsPath != null) {
            if (grpcPort != null) {
                throw new Exception("Must provide at most one of " +
                                    GRPC_PORT_ENV_VAR + " and " + GRPC_UDS_PATH_ENV_VAR);
            }
            ManagedChannel channel = udsPath.equals(servingUdsPath) ? servingChannel
                    : buildLocalChannel(-1, udsPath, maxGrpcMessageSize);
            candidates = Collections.singletonList(channel);
            logger.info("Using grpc domain socket path " + udsPath
                        + " for management connection to collocated ModelServer container");
        } else if (grpcPort != null) {
            ManagedChannel channel = grpcPort.equals(servingPort) ? servingChannel
                    : buildLocalChannel(Integer.parseInt(grpcPort), null, maxGrpcMessageSize);
            candidates = Collections.singletonList(channel);
            logger.info("Using grpc port " + grpcPort
                        + " for management connection to collocated ModelServer container");
        } else {
            ManagedChannel ipChannel = buildLocalChannel(DEFAULT_GRPC_PORT, null, maxGrpcMessageSize);
            candidates = !NettyCommon.useEpoll() ? Collections.singletonList(ipChannel)
                    : Arrays.asList(buildLocalChannel(-1, DEFAULT_UDS_PATH, maxGrpcMessageSize), ipChannel);
            logger.info("Collocated ModelServer container management connection env var"
                        + " not provided, will attempt to use defaults: socket=" + DEFAULT_UDS_PATH + ", port="
                        + DEFAULT_GRPC_PORT);
        }

        modelLoader = new ExternalModelLoader(candidates, servingChannel);

        long before = System.nanoTime();
        RuntimeStatusResponse rsr =
                modelLoader.waitForModelServerStart(MODEL_SERVICE_STARTUP_TIMEOUT_SECS, 2); // wait for 3 mins

        long defaultModelSize = rsr.getDefaultModelSizeInBytes();
        int loadingConcurrency = rsr.getMaxLoadingConcurrency();
        int loadTimeoutMs = rsr.getModelLoadingTimeoutMs();
        long capacityBytes = rsr.getCapacityInBytes();
        boolean limitModelConc = rsr.getLimitModelConcurrency();

        logger.info("Model Server startup completed within " + msSince(before) + "ms");
        logger.info("Reported capacity of " + (capacityBytes / Mi) + "MiB, default model size of "
                    + (defaultModelSize / Mi) + "MiB, max loading concurrency of " + loadingConcurrency
                    + ", model loading timeout of " + loadTimeoutMs + "ms, limit model concurrency = " + limitModelConc);

        return new LocalInstanceParameters(modelLoader.defaultModelSizeUnits).maxLoadingConcurrency(loadingConcurrency)
                .modelLoadTimeoutMs(loadTimeoutMs).useNonHeap(capacityBytes).limitModelConcurrency(limitModelConc);
    }

//	EventLoopGroup getEventLoopGroup() {
//	    return modelLoader != null ? modelLoader.eventLoopGroup : null;
//	}

    @Override
    protected void shutdown() {
        if (modelLoader != null) {
            modelLoader.shutdown();
        }
        super.shutdown();
        if (modelLoader != null) {
            modelLoader.shutdownNow();
        }
    }

    @Override
    protected String getRequestMethodName(Method method, Object[] args) {
        if (method == directLocalMeth) {
            return (String) args[0];
        }
        return ThreadContext.getContextEntry(TAS_GRPC_METH_CXT_KEY);
    }

    Channel servingChannel;
    Map<String, int[]> idInjectionPaths = Collections.emptyMap();
    boolean anyMethodAllowed;

    class ExternalModel {
        final String modelId;
        final boolean isAscii;

        // changes to loadFuture, unloadFuture,
        // count, and waiters are guarded by lock
        ListenableFuture<LoadModelResponse> loadFuture; // non-null if waiting to load, loaded or failed
        ListenableFuture<Boolean> unloadFuture; // non-null iff unload is in-progress

        // a corresponding unloadModel will always follow a loadModel, but in race conditions
        // "pairs" of these could be interleaved. The count field is analogous to a reference
        // count, incremented when loadModels received and decremented on unloadModels
        int count = 1;

        // this keeps track of how many threads are currently blocking in a loadModel call,
        // and so will always be <= count
        int waiters = 1;

        // both futures are null and count == 0, iff this object has been removed from the
        // models map

        public ExternalModel(String modelId) {
            this.modelId = modelId;
            this.isAscii = GrpcSupport.isVisibleAscii(modelId);
        }

        public List<ByteBuffer> applyModel(List<ByteBuffer> input, Map<String, String> metadata)
                throws ApplierException {
            String grpcMethod = ThreadContext.getContextEntry(TAS_GRPC_METH_CXT_KEY);
            if (grpcMethod == null) {
                throw new ApplierException("Missing method name on internal rpc", null, Code.INTERNAL.name());
            }
            if (logModelInvocations || logger.isDebugEnabled()) {
                Level level = logModelInvocations ? Level.INFO : Level.DEBUG;
                logger.log(level, "invoking model runtime gRPC method: " + grpcMethod);
            }
            try {
                return callCustomMethod(grpcMethod, input);
            } catch (Exception e) {
                throw handleGrpcException(grpcMethod, e);
            }
        }

        private ApplierException handleGrpcException(String grpcMethod, Exception e) {
            Status grpcStatus = Status.fromThrowable(e);
            Code code = grpcStatus.getCode();
            // Triton Inference Server incorrectly returns UNAVAILABLE in the not found case,
            // See https://github.com/triton-inference-server/server/issues/3399
            if (code == Code.UNAVAILABLE) {
                String message = grpcStatus.getDescription();
                if (message != null && message.startsWith("Request for unknown model") &&
                    (message.endsWith(" is not found") || message.endsWith(" has no available versions"))) {
                    code = Code.NOT_FOUND; // treat as if NOT_FOUND code was returned
                }
            }
            // Seldon MLServer incorrectly returns INVALID_ARGUMENT in the not found case
            else if (code == Code.INVALID_ARGUMENT) {
                String message = grpcStatus.getDescription();
                if (message != null && message.startsWith("Model ") && message.endsWith(" not found")) {
                    code = Code.NOT_FOUND; // treat as if NOT_FOUND code was returned
                }
            }
            if (code == Code.NOT_FOUND) {
                modelLoader.handleServingNotFound(this);
            } else if (code == Code.UNKNOWN && isInterruption(e)) {
                Deadline deadline = Context.current().getDeadline();
                code = deadline != null && deadline.isExpired() ? Code.DEADLINE_EXCEEDED : Code.CANCELLED;
            }
            logger.error("Error invoking " + grpcMethod + " method on model " + modelId + ": "
                         + GrpcSupport.toShortString(grpcStatus));
            ApplierException ae = new ApplierException(grpcMethod + ": " + e.getMessage(), null, code.name());
            ae.initCause(e);
            // Set causestacktrace string after trimming the whole chain so that it's smaller
            return trimStack(ae, true).setCauseStacktrace(Throwables.getStackTraceAsString(e));
        }

        public ModelResponse callModel(String methodName, Metadata headers, ByteBuf data) throws ApplierException {
            if (logModelInvocations || logger.isDebugEnabled()) {
                Level level = logModelInvocations ? Level.INFO : Level.DEBUG;
                logger.log(level, "invoking model runtime gRPC method: " + methodName);
            }
            try {
                data.readerIndex(0); // rewind buffer in case of retries
                //TODO there is a special case bug here - if the runtime reports
                // NOT_FOUND we may end up reloading/invoking again as part of this
                // same request path in this same instance, but the headers may not
                // be safe to reuse since they've already been passed to ClientCall.start()
                // once, which mangles them (e.g. base64-encoding binary vals in-place).
                // A (the?) solution would be to always copy the metadata before calling,
                // but this adds overhead to the 99.9% case.
                headers = GrpcSupport.replaceHeaders(headers, modelId, isAscii);
                return callCustomMethod(methodName, headers, data);
            } catch (Exception e) {
                throw handleGrpcException(methodName, e);
            }
        }

        private List<ByteBuffer> callCustomMethod(String methodName, List<ByteBuffer> input) throws Exception {
            for (ByteBuffer bb : input) {
                bb.rewind(); // in case of retries
            }
            // extract request metadata
            Metadata headers = GrpcSupport.deserializeMetadata(input.get(0), modelId, isAscii);
            Context newContext = null, prevContext = null;
            ModelResponse response;
            long nanosToDeadline = ThreadContext.nanosUntilDeadline();
            if (nanosToDeadline >= 0L) {
                if (nanosToDeadline < 5L) {
                    throw Status.DEADLINE_EXCEEDED.asRuntimeException();
                }
                newContext = Context.current().withDeadlineAfter(nanosToDeadline, NANOSECONDS, taskPool);
                prevContext = newContext.attach();
            }
            try {
                response = callCustomMethod(methodName, headers, GrpcSupport.toMessage(input));
            } finally {
                if (newContext != null) {
                    newContext.detach(prevContext);
                }
            }
            ByteBuf headerBuf = GrpcSupport.serializeMetadata(response.metadata, true);
            ReleaseAfterResponse.addReleasable(headerBuf);
            return GrpcSupport.toBbList(headerBuf, response.data);
        }

        private ModelResponse callCustomMethod(String methodName, Metadata headers, ByteBuf data) throws Exception {
            assert methodName != null;
            // Block attempts to call the internal ModelRuntime management service via inference path
            if (methodName.startsWith(MGMT_SERVICE_PREFIX)) {
                throw asLeanException(Status.UNIMPLEMENTED.withDescription(
                        "Method not found or not permitted: " + methodName));
            }

            // The idInjectionPaths map is used as an allow-list. Methods containing an empty
            // int array are permitted but do not require model id injection.
            final int[] injectIdPath = idInjectionPaths.get(methodName);
            final byte[] idBytesToInject;
            if (injectIdPath == null) {
                if (!anyMethodAllowed) {
                    throw asLeanException(Status.UNIMPLEMENTED.withDescription(
                            "Method not found or not permitted: " + methodName));
                }
                idBytesToInject = null;
            } else if (injectIdPath.length == 0) {
                idBytesToInject = null;
            } else {
                // This must be done prior to passing the headers to clientCall.start()
                idBytesToInject = headers.get(GrpcSupport.MODEL_ID_BIN_HEADER_BYTES);
                if (idBytesToInject == null) {
                    throw asLeanException(Status.INTERNAL.withDescription("model-id-bin header not found"));
                }
            }

            MethodDescriptor<ByteBuf, ByteBuf> md = GrpcSupport.getMethodDescriptorIfPresent(methodName);
            final boolean mdCached = md != null;
            // only store descriptor after successful RPC (prevent filling cache with invalid descriptors)
            final MethodDescriptor<ByteBuf, ByteBuf> methodDescriptor = mdCached ? md
                    : GrpcSupport.makeMethodDescriptor(methodName);

            // Not setting Deadline explicitly here,
            // relying on grpc Context for setting/propagating that
            ClientCall<ByteBuf, ByteBuf> clientCall = servingChannel.newCall(methodDescriptor, DIRECT_EXEC_CALL_OPTS);

            SettableFuture<ModelResponse> resultFuture = SettableFuture.create();
            clientCall.start(new ClientCall.Listener<>() {
                Metadata headers;
                ByteBuf response;

                @Override
                public void onMessage(ByteBuf message) {
                    if (response != null) {
                        message.release();
                        String msg = "Streaming not yet supported";
                        resultFuture.setException(new IllegalStateException(msg));
                        clientCall.cancel(msg, null);
                    } else {
                        response = message;
                    }
                }

                @Override
                public void onHeaders(Metadata headers) {
                    this.headers = headers;
                }

                @Override
                public void onClose(Status status, Metadata trailers) {
                    try {
                        if (!status.isOk()) {
                            resultFuture.setException(asLeanException(status));
                        } else if (response == null) {
                            resultFuture.setException(new IllegalStateException(
                                    "no response received from model runtime"));
                        } else {
                            try {
                                if (!resultFuture.set(new ModelResponse(headers, response.retain()))) {
                                    response.release();
                                }
                                if (!mdCached) {
                                    GrpcSupport.addMethodDescriptor(methodDescriptor);
                                }
                            } catch (Throwable t) {
                                if (!resultFuture.setException(t) || t instanceof Error) {
                                    throw t;
                                }
                            }
                        }
                    } finally {
                        if (response != null) {
                            response.release();
                            response = null;
                        }
                    }
                }
            }, headers);
            headers = null; // must not access after passing to clientCall.start()

            data.retain();
            try {
                if (idBytesToInject != null) {
                    data = ProtoSplicer.spliceId(data, injectIdPath, idBytesToInject);
                }
                clientCall.sendMessage(data);
                data = null;
            } finally {
                if (data != null) {
                    data.release();
                }
            }
            clientCall.halfClose();
            // ensure it's not a streaming response
            clientCall.request(2);
            try {
                ModelResponse mr = resultFuture.get(); //TODO safeguard timeout tbd
                mr.releaseAfterResponse();
                return mr;
            } catch (InterruptedException ie) {
                // must ensure buffer gets released
                Futures.addCallback(resultFuture, (FutureListener<ModelResponse>) (mr, t) -> {
                    if (mr != null) {
                        mr.release();
                    }
                }, directExecutor());
                clientCall.cancel(null, ie);
                throw ie;
            } catch (ExecutionException ee) {
                // if resultFuture completes exceptionally then bufs will have been released
                throw throwCause(ee);
            }
        }
    }

    static class UnloadFailure {
        final ExternalModel model;
        final SettableFuture<UnloadModelResponse> retryFuture = SettableFuture.create();
        final int count;

        public UnloadFailure(ExternalModel model, int count) {
            this.model = model;
            this.count = count;
        }
    }

    class ExternalModelLoader extends ModelLoader<ExternalModel> {
        // Possible model-server management endpoints to try
        private final List<ModelRuntime> candidates;

        // Set upon successful completion of waitForModelServerStart method
        private /*final*/ ModelRuntime modelRuntime;
        // Set either on construction or unpon successful completion of waitForModelServerStart method
        private /*final*/ ManagedChannel servingChannel;

        //final EventLoopGroup eventLoopGroup;

        // Separate map here represents state of models within external runtime,
        // regardless of state in main cache. It's an edge case that a model could be
        // evicted from the main cache and immediately added back to the cache
        // (triggering a new load), prior to the eviction unload starting.
        // This case will be "caught" here via the reference counting in this map.
        // It also catches the potentially more common case (although still likely rare)
        // where a load comes in for a model which is in the process of unloading.
        final ConcurrentMap<String, ExternalModel> models = new ConcurrentHashMap<>(64);

        // This queue should generally not be needed/used - it's used to manage retries
        // of models which fail to unload due to runtime unavailability (UNAVAILABLE errors)
        // which could occur if the model runtime restarts for some reason.
        final Queue<UnloadFailure> unloadsToRetry = new ConcurrentLinkedQueue<>();

        @GuardedBy("unloadsToRetry")
        ListenableScheduledFuture<?> unloadRetryTask;

        /**
         * @param channelCandidates
         * @param servingChannel
         */
        public ExternalModelLoader(List<ManagedChannel> channelCandidates, ManagedChannel servingChannel) {
            this.candidates = new ArrayList<>(Lists.transform(channelCandidates, MmModelRuntime::new));
            this.servingChannel = servingChannel;
        }

        public void shutdown() {
            UnloadFailure toRetry;
            while ((toRetry = unloadsToRetry.poll()) != null) {
                toRetry.retryFuture.setException(Status.ABORTED.withDescription("shutting down").asRuntimeException());
            }
            synchronized (this) {
                ModelRuntime mr = modelRuntime;
                ManagedChannel channel = mr != null ? mr.getChannel() : null;
                if (channel != null) {
                    channel.shutdown();
                }
                ManagedChannel sChannel = servingChannel;
                if (sChannel != null && sChannel != channel) {
                    sChannel.shutdown();
                }
                for (ModelRuntime candidate : candidates) {
                    candidate.getChannel().shutdown();
                }
            }
        }

        public synchronized void shutdownNow() {
            ModelRuntime mr = modelRuntime;
            ManagedChannel channel = mr != null ? mr.getChannel() : null;
            if (channel != null) {
                channel.shutdownNow();
            }
            ManagedChannel sChannel = servingChannel;
            if (sChannel != null && sChannel != channel) {
                sChannel.shutdownNow();
            }
            for (ModelRuntime candidate : candidates) {
                candidate.getChannel().shutdownNow();
            }
        }

        public RuntimeStatusResponse waitForModelServerStart(int timeoutSecs, int pollIntervalSecs) throws Exception {
            //TODO better handle case where model server process dies during startup

            Deadline deadline = Deadline.after(timeoutSecs, SECONDS);
            Exception[] unavailableExceptions = new Exception[candidates.size()];
            boolean receivedNonError = false;
            while (true) {
                for (int c = 0; c < candidates.size(); c++) {
                    ModelRuntime candidate = candidates.get(c);
                    try {
                        if (deadline.isExpired()) {
                            throw Status.DEADLINE_EXCEEDED.asRuntimeException();
                        }
                        RuntimeStatusResponse rsr = candidate.status(deadline);
                        receivedNonError = true;
                        switch (rsr.getStatus()) {
                        case READY:
                            defaultModelSizeUnits = toUnits(rsr.getDefaultModelSizeInBytes());
                            if (rsr.getMethodInfosCount() > 0) {
                                logger.info("Model Server provided MethodInfos: "
                                      + Maps.transformValues(rsr.getMethodInfosMap(), mi -> "[idFieldPath: "
                                      + (mi.getIdInjectionPathCount() == 0 ? "(none)" :
                                        COMMA_JOIN.join(mi.getIdInjectionPathList())) + "]"));
                                Map<String, int[]> map = new HashMap<>(8);
                                for (Entry<String, MethodInfo> ent : rsr.getMethodInfosMap().entrySet()) {
                                    MethodInfo mi = ent.getValue();
                                    int count = mi != null ? mi.getIdInjectionPathCount() : 0;
                                    // Empty path means no id injection but that method is permitted when
                                    // anyMethodAllowed == false
                                    int[] path = count == 0 ? EMPTY_INT_ARRAY
                                            : IntStream.range(0, count).map(mi::getIdInjectionPath).toArray();
                                    map.put(ent.getKey(), path);
                                }
                                idInjectionPaths = ImmutableMap.copyOf(map);
                                anyMethodAllowed = rsr.getAllowAnyMethod();
                            } else {
                                // We assume that all methods are permitted if no methodInfos map is provided
                                anyMethodAllowed = true;
                            }
                            synchronized (this) {
                                modelRuntime = candidate;
                                candidates.remove(c);
                                if (servingChannel == null) {
                                    servingChannel = candidate.getChannel();
                                }
                                // If there were other candidates, shutdown their channels
                                for (ModelRuntime other : candidates) {
                                    other.getChannel().shutdown();
                                }
                                candidates.clear();
                            }
                            SidecarModelMesh.this.servingChannel = servingChannel;
                            logger.info("Model Server returned READY status on channel " + candidate.getChannel());
                            return rsr;
                        case STARTING:
                            logger.info("Model Server returned STARTING status...");
                            // continue polling
                            break;
                        case FAILING:
                        case UNRECOGNIZED:
                        default:
                            throw new Exception("Model Server reported failing "
                                                + "or unrecognized status during startup: " + rsr.getStatus());
                        }
                    } catch (StatusRuntimeException sre) {
                        Code code = sre.getStatus().getCode();
                        if (code == Status.Code.DEADLINE_EXCEEDED) {
                            TimeoutException te = new TimeoutException(
                                    "Model server container did not start after " + timeoutSecs + " seconds");
                            // Add prior observed UNAVAILABLE exception stacks to help with possible problem determination
                            if (!receivedNonError) for (Exception e : unavailableExceptions) if (e != null) {
                                if (te.getCause() == null) te.initCause(e);
                                else te.addSuppressed(e);
                            }
                            throw te;
                        }
                        if (code == Status.Code.UNIMPLEMENTED) {
                            if (!(candidate instanceof TasModelRuntime)) {
                                logger.warn("Model Server does not implement mmesh.ModelRuntime interface, "
                                            + " switching to use legacy tas-runtime ModelServer interface");
                                synchronized (this) {
                                    candidates.set(c, candidate = new TasModelRuntime(candidate.getChannel()));
                                }
                                continue;
                            } else {
                                throw new Exception(
                                        "Model server container does not " + "implement mmesh.ModelRuntime interface");
                            }
                        }
                        if (code == Status.Code.UNAVAILABLE) {
                            if (!receivedNonError) {
                                unavailableExceptions[c] = sre;
                            }
                            logger.info("Waiting for Model Server process to become available...");
                        } else {
                            logger.warn("Model Server startup poll failed with error: " + sre,
                                    sre.getCause() != null ? sre.getCause() : null);
                        }
                    }
                }
                Thread.sleep(pollIntervalSecs * 1000L);
            }
        }

        protected int defaultModelSizeUnits = 0;

        protected boolean predictUnimplemented;

        @Override
        public int predictSize(String modelId, ModelInfo modelInfo) {
            if (!predictUnimplemented && modelId != null) {
                try {
                    PredictModelSizeRequest.Builder bld = PredictModelSizeRequest.newBuilder().setModelId(modelId);
                    if (modelInfo.getServiceType() != null) bld.setModelType(modelInfo.getServiceType());
                    if (modelInfo.getModelPath() != null) bld.setModelPath(modelInfo.getModelPath());
                    if (modelInfo.getEncKey() != null) bld.setModelKey(modelInfo.getEncKey());
                    return toUnits(modelRuntime.predictSize(bld.build()).getSizeInBytes());
                } catch (StatusRuntimeException sre) {
                    if (sre.getStatus().getCode() == Code.UNIMPLEMENTED) {
                        predictUnimplemented = true;
                    } else {
                        throw sre;
                    }
                }
            }
            return defaultModelSizeUnits;
        }

        @Override
        public LoadedRuntime<ExternalModel> loadRuntime(String modelId, ModelInfo modelInfo) throws Exception {
            ExternalModel newModel = new ExternalModel(modelId);

            ListenableFuture<LoadModelResponse> lf;
            ExternalModel model;
            while (true) {
                synchronized (newModel) {
                    model = models.putIfAbsent(modelId, newModel);
                    if (model == null) {
                        model = newModel;
                        model.loadFuture = lf = submitLoad(modelId, modelInfo);
                        break;
                    }
                    // Else the model is already loaded or loading, return the existing future.
                    //TODO there is an unlikely race condition which could in theory happen if
                    // a model-id was deregistered and *immediately* re-registered with different
                    // parameters... where we got the second load request before the unload
                    // corresponding to the first eviction. This is hopefully only theoretical
                    // given that model ids should be treated in an immutable manner, but maybe
                    // we should store/compare the ModelInfo to at least detect and fail loading
                    // in this case.
                }
                synchronized (model) {
                    lf = model.loadFuture;
                    if (lf == null && model.unloadFuture == null) {
                        if (model.count <= 0) {
                            continue; // must have been removed from map
                        }
                    }
                    model.count++;
                    model.waiters++;

                    if (lf == null) {
                        if (model.unloadFuture == null) {
                            lf = submitLoad(modelId, modelInfo);
                        } else {
                            // Wait for unload if in progress, and if fails (assumed permanently) then
                            // propagate the failure wrapped in a FAILED_PRECONDITION StatusException -
                            // indicating that load was not attempted
                            lf = Futures.transformAsync(Futures.catchingAsync(
                                    Futures.nonCancellationPropagating(model.unloadFuture), Exception.class, e -> {
                                        throw Status.FAILED_PRECONDITION.withCause(e)
                                                .withDescription(
                                                        "Prior unload of model " + modelId + " failed unrecoverably")
                                                .asException();
                                    }, directExecutor()), umr -> submitLoad(modelId, modelInfo), directExecutor());
                        }
                        model.loadFuture = lf;
                    }
                }
                break;
            }

            boolean isInterrupted = false;
            try {
                LoadModelResponse lmr = lf.get();
                return new LoadedRuntime<>(model, Math.max(0, toUnits(lmr.getSizeInBytes())), lmr.getMaxConcurrency());
            } catch (InterruptedException ie) {
                isInterrupted = true;
                throw ie;
            } catch (ExecutionException ee) {
                throw throwCause(ee);
            } finally {
                synchronized (model) {
                    // if the last "waiter" is cancelled (via an interrupt), then
                    // it's ok to preemptively cancel the load (an unloadModel()
                    // call should follow)
                    if (--model.waiters == 0 && isInterrupted) {
                        lf.cancel(true);
                    }
                }
            }
        }

        private ListenableFuture<LoadModelResponse> submitLoad(String modelId, ModelInfo modelInfo) {
            return callInForkedContextUnchecked(() -> {
                processUnloadRetryQueue(false);
                LoadModelRequest.Builder bld = LoadModelRequest.newBuilder().setModelId(modelId);
                if (modelInfo.getServiceType() != null) bld.setModelType(modelInfo.getServiceType());
                if (modelInfo.getModelPath() != null) bld.setModelPath(modelInfo.getModelPath());
                if (modelInfo.getEncKey() != null) bld.setModelKey(modelInfo.getEncKey());
                //TODO could intercept ALREADY_EXISTS here and treat as success (modelSize will then
                // be called separately), *except that* we might still need the value of maxConcurrency
                return modelRuntime.load(bld.build());
            });
        }

        @Override
        public int modelSize(ExternalModel model) throws Exception {
            return toUnits(modelRuntime.size(ModelSizeRequest.newBuilder().setModelId(model.modelId).build())
                    .getSizeInBytes());
        }

        /**
         * Asynchronous
         */
        @Override
        public ListenableFuture<Boolean> unloadModel(String modelId) {
            while (true) {
                ExternalModel model = models.get(modelId);
                if (model == null) {
                    return COMPLETED;
                }
                synchronized (model) {
                    if (model.loadFuture != null || model.unloadFuture != null || model.count > 0) {
                        return unloadModel(model);
                    }
                    // else must have been removed
                }
            }
        }

        public ListenableFuture<Boolean> unloadModel(final ExternalModel model) {
            if (model == null) {
                return COMPLETED;
            }
            synchronized (model) {
                //assert model.loadFuture != null || model.unloadFuture != null;
                if (--model.count > 0) {
                    // This case is where a model is removed from the local cache
                    // (explicitly or due to eviction), and then immediately added
                    // (for example due to a new incoming request for the model)
                    // and the load from the add overtakes the unload corresponding
                    // to the remove. We allow the out-of-order calls to cancel
                    // each other out instead of performing unload, load.
                    return COMPLETED;
                }
                if (model.loadFuture != null) {
                    if (!model.loadFuture.isDone()) {
                        model.loadFuture.cancel(true);
                    }
                    model.loadFuture = null;
                }
                if (model.unloadFuture != null) {
                    // This case is where the prior load to which this unload corresponds
                    // must have been rejected due to an existing unload failure for the
                    // same model (which may have retries in progress, or may be already
                    // be "permanently" failed). The FALSE value indicates "vacuous" success
                    return Futures.immediateFuture(Boolean.FALSE);
                }
                return model.unloadFuture = submitUnload(model);
            }
        }

        private ListenableFuture<Boolean> submitUnload(ExternalModel model) {
            return callInForkedContextUnchecked(() -> attemptUnload(model, true, 0));
        }

        // must be careful not to call this from an external request grpc Context
        // (fork first if necessary)
        private ListenableFuture<Boolean> attemptUnload(ExternalModel model, boolean first, int failCount) {
            // TODO comment on exception case
            return FluentFuture
                    .from(modelRuntime.unload(UnloadModelRequest.newBuilder().setModelId(model.modelId).build()))
                    .catchingAsync(Exception.class, e -> {
                        Status status = Status.fromThrowable(e);
                        Code code = status.getCode();
                        // NOT_FOUND from unload is equivalent to successful
                        if (code == Code.NOT_FOUND) {
                            return UNLOAD_COMPLETE;
                        }
                        // Record failed unload attempt (final failure recorded only in superclass)
                        metrics.logCounterMetric(Metric.UNLOAD_MODEL_ATTEMPT_FAILURE);

                        // If the unload failed, queue here for retry (will be retried prior to any
                        // subsequent load attempt, and after any subsequent *successful* unload)
                        if (failCount < UNLOAD_RETRIES && !shuttingDown) {
                            // Also retry arbitrary failures, but only a finite number of times
                            if (failCount % 6 == 0) {
                                logger.warn("Unload of model " + model.modelId + " failed, queueing "
                                            + (UNLOAD_RETRIES - failCount) + " more time(s) for retry: "
                                            + GrpcSupport.toShortString(status));
                            }
                            return enqueueUnloadFailure(model, failCount + 1);
                        }
                        // If retries are exhausted fail the unload, which is treated as unrecoverable
                        throw e;
                    }, directExecutor()).transform(umr -> {
                        if (!first) {
                            logger.info("Model " + model.modelId + " unloaded successfully following retry");
                        }
                        if (fullUnloadRetryRateLimiter.tryAcquire()) {
                            processUnloadRetryQueue(true);
                        }
                        synchronized (model) {
                            model.unloadFuture = null;
                            if (model.count == 0) {
                                // assert model.loadFuture == null
                                models.remove(model.modelId, model);
                            }
                            return Boolean.TRUE;
                        }
                    }, directExecutor());
        }

        private ListenableFuture<UnloadModelResponse> enqueueUnloadFailure(ExternalModel model, int failCount) {
            UnloadFailure retryAttempt = new UnloadFailure(model, failCount);
            synchronized (unloadsToRetry) {
                unloadsToRetry.add(retryAttempt);
                if (unloadRetryTask == null) {
                    try {
                        unloadRetryTask = taskPool.scheduleAtFixedRate(() -> {
                            if (!processUnloadRetryQueue(false)) {
                                synchronized (unloadsToRetry) {
                                    // queue was empty
                                    if (unloadRetryTask != null && unloadsToRetry.isEmpty()) {
                                        unloadRetryTask.cancel(false);
                                        unloadRetryTask = null;
                                    }
                                }
                            }
                        }, 1, 10, SECONDS);
                    } catch (RejectedExecutionException ree) {
                        return Futures.immediateFailedFuture(ree); // shutting down
                    }
                }
            }
            return retryAttempt.retryFuture;
        }

        private boolean processUnloadRetryQueue(boolean all) {
            UnloadFailure toRetry;
            boolean processed = false;
            while ((toRetry = unloadsToRetry.poll()) != null) {
                toRetry.retryFuture.setFuture(Futures.transform(attemptUnload(toRetry.model, false, toRetry.count),
                    r -> UnloadModelResponse.getDefaultInstance(), directExecutor()));
                processed = true;
                if (!all) break;
            }
            return processed;
        }

        // Called when an unexpected NOT_FOUND is returned from an inferencing request
        // to the runtime container. This could happen for an example if that container
        // is killed and restarts.
        private void handleServingNotFound(ExternalModel model) {
            final String modelId = model.modelId;
            if (modelRuntime.getChannel() == servingChannel) {
                // If model management and serving are handled by a single process
                // we assume no cleanup is needed and just remove from our parallel map
                boolean removed = models.remove(modelId, model);
                if (removed) {
                    logger.warn("Removed sidecar record for model " + modelId
                                + " after unexpected NOT_FOUND received from inference request");
                }
            } else if (model.unloadFuture == null) {
                synchronized (model) {
                    // Else we send a "cleanup" unload request
                    if (model.unloadFuture == null && models.get(modelId) == model) {
                        if (model.loadFuture != null) {
                            if (!model.loadFuture.isDone()) {
                                model.loadFuture.cancel(true);
                            }
                            model.loadFuture = null;
                        }
                        model.count = 0;
                        model.unloadFuture = submitUnload(model);
                        logger.warn("Triggered \"cleanup\" unload for model " + modelId
                                    + " after unexpected NOT_FOUND received from inference request");
                    }
                }
            }
        }
    }

    static ManagedChannel buildLocalChannel(int port, String domainSocketPath, int maxMessageSize) {
        NettyChannelBuilder ncb;
        if (domainSocketPath != null) {
            if (!NettyCommon.useEpoll()) {
                throw new RuntimeException(
                        "Epoll must be available to use domain socket sidecar address" + domainSocketPath);
            }
            ncb = NettyChannelBuilder.forAddress(new DomainSocketAddress(domainSocketPath))
                    .channelType(EpollDomainSocketChannel.class);
        } else {
            ncb = NettyChannelBuilder.forAddress("localhost", port).channelType(NettyCommon.getChannelClass());
        }
        // share common worker ELG with litelinks
        ncb.eventLoopGroup(NettyCommon.getWorkerGroup()).usePlaintext();

        if (maxMessageSize > 0) {
            ncb.maxInboundMessageSize(maxMessageSize);
        }

        //TODO later probably make these configurable
        InternalNettyChannelBuilder.setStatsEnabled(ncb, false);
        InternalNettyChannelBuilder.setTracingEnabled(ncb, false);
        return ncb.build();
    }

    static Exception throwCause(ExecutionException ee) throws Exception {
        Throwables.throwIfInstanceOf(ee.getCause(), Exception.class);
        Throwables.throwIfUnchecked(ee.getCause());
        throw new RuntimeException(ee.getCause());
    }

    static <V> V callInForkedContextUnchecked(Callable<V> callable) {
        try {
            return Context.current().fork().call(callable);
        } catch (Exception e) {
            Throwables.throwIfInstanceOf(e, RuntimeException.class);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected ModelLoader<?> getLoader() {
        return modelLoader;
    }

    protected static final Method localMeth = getMethod(ExternalModel.class, "applyModel");
    protected static final Method remoteMeth = getMethod(ModelMeshService.Iface.class, "applyModelMulti");

    protected static final Method directLocalMeth = getMethod(ExternalModel.class, "callModel");

    @SuppressWarnings("unchecked")
    @Override
    protected Object invokeLocalMethod(String modelId, Method method, Object runtime, Object[] args)
            throws ApplierException {
        ExternalModel model = (ExternalModel) runtime;
        return method == localMeth ? model.applyModel((List<ByteBuffer>) args[0], (Map<String, String>) args[1])
                : model.callModel((String) args[0], (Metadata) args[1], (ByteBuf) args[2]);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object invokeRemoteModel(BaseModelMeshService.Iface client, Method method, Method remoteMeth,
            String modelId, Object... args) throws Exception {
        if (method != directLocalMeth) {
            return super.invokeRemoteModel(client, method, remoteMeth, modelId, args);
        }
        String methodName = (String) args[0];
        Deadline deadline = Context.current().getDeadline();
        if (deadline != null) {
            long nanosToDeadline = deadline.timeRemaining(NANOSECONDS);
            if (nanosToDeadline < 5L) {
                throw trimStack(new ApplierException(methodName, null, Code.DEADLINE_EXCEEDED.name()), false);
            }
            ThreadContext.setDeadlineAfter(nanosToDeadline);
        }
        ByteBuf dataIn = ((ByteBuf) args[2]).readerIndex(0);
        ThreadContext.addContextEntry(TAS_GRPC_METH_CXT_KEY, methodName);
        ByteBuf headersBuf = GrpcSupport.serializeMetadata((Metadata) args[1], true);
        try {
            List<ByteBuffer> bbufsIn = GrpcSupport.toBbList(headersBuf, dataIn);
            List<ByteBuffer> bbufsOut = (List<ByteBuffer>) remoteMeth.invoke(client, modelId, bbufsIn, null);
            Metadata metaOut = GrpcSupport.deserializeMetadata(bbufsOut.get(0), null, false);
            return new ModelResponse(metaOut, GrpcSupport.toMessage(bbufsOut));
        } finally {
            headersBuf.release();
        }
    }

    @Idempotent
    @Override
    public List<ByteBuffer> applyModelMulti(String modelId, List<ByteBuffer> input, Map<String, String> metadata)
            throws TException {
        try {
            return applyModel(modelId, input, metadata);
        } catch (ModelNotHereException | ModelNotFoundException e) {
            throw e;
        } catch (Exception e) {
            // don't log interruption stacks
            if (!(e instanceof ApplierException) || !isInterruption(e)) {
                logger.warn("applyModel throwing exception for model " + modelId, e);
            } else {
                logger.info("applyModel call interrupted for model " + modelId);
            }
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    List<ByteBuffer> applyModel(String modelId, List<ByteBuffer> input, Map<String, String> metadata)
            throws TException {
        return (List<ByteBuffer>) invokeModel(modelId, localMeth, remoteMeth, input, metadata);
    }

    // refcount of provided ByteBuf should not be modified
    ModelResponse callModel(String modelId, String methodName, Metadata headers, ByteBuf data) throws TException {
        return (ModelResponse) invokeModel(modelId, directLocalMeth, remoteMeth, methodName, headers, data);
    }

    @Idempotent
    @Override
    @Deprecated
    public ByteBuffer applyModel(String modelId, ByteBuffer input, Map<String, String> metadata) throws TException {
        ByteBuffer metaBuf = input.duplicate();
        GrpcSupport.skipMetadata(input);
        metaBuf.limit(input.position());
        List<ByteBuffer> resp = applyModelMulti(modelId, Arrays.asList(metaBuf, input.slice()), null);
        ByteBuffer ret = ByteBuffer.allocate(resp.stream().mapToInt(ByteBuffer::remaining).sum());
        for (ByteBuffer bb : resp) {
            ret.put(bb);
        }
        ret.flip();
        return ret;
    }

    // Scaffolding below to support backwards compatibility of legacy TAS ModelServer gRPC interface for now,
    // will be removed in future update

    interface ModelRuntime {
        RuntimeStatusResponse status(Deadline deadline);
        ModelSizeResponse size(ModelSizeRequest request);
        PredictModelSizeResponse predictSize(PredictModelSizeRequest request);
        ListenableFuture<LoadModelResponse> load(LoadModelRequest request);
        ListenableFuture<UnloadModelResponse> unload(UnloadModelRequest request);
        ManagedChannel getChannel();
    }

    static class MmModelRuntime implements ModelRuntime {
        final ModelRuntimeBlockingStub bstub;
        final ModelRuntimeFutureStub fstub;

        public MmModelRuntime(ManagedChannel channel) {
            bstub = ModelRuntimeGrpc.newBlockingStub(channel);
            fstub = ModelRuntimeGrpc.newFutureStub(channel);
        }

        @Override
        public RuntimeStatusResponse status(Deadline deadline) {
            return bstub.withDeadline(deadline).runtimeStatus(RuntimeStatusRequest.getDefaultInstance());
        }
        @Override
        public ModelSizeResponse size(ModelSizeRequest request) {
            return bstub.withDeadlineAfter(SIZING_TIMEOUT_SECS, SECONDS).modelSize(request);
        }
        @Override
        public PredictModelSizeResponse predictSize(PredictModelSizeRequest request) {
            return bstub.withDeadlineAfter(PREDICT_SIZE_TIMEOUT_MS, MILLISECONDS).predictModelSize(request);
        }
        @Override
        public ListenableFuture<LoadModelResponse> load(LoadModelRequest request) {
            return fstub.loadModel(request);
        }
        @Override
        public ListenableFuture<UnloadModelResponse> unload(UnloadModelRequest request) {
            return fstub.unloadModel(request);
        }
        @Override
        public ManagedChannel getChannel() {
            return (ManagedChannel) bstub.getChannel();
        }
    }

    @Deprecated
    static class TasModelRuntime implements ModelRuntime {
        final ModelServerBlockingStub bstub;
        final ModelServerFutureStub fstub;

        public TasModelRuntime(ManagedChannel channel) {
            bstub = ModelServerGrpc.newBlockingStub(channel);
            fstub = ModelServerGrpc.newFutureStub(channel);
        }

        @Override
        public RuntimeStatusResponse status(Deadline deadline) {
            return bstub.withDeadline(deadline).runtimeStatus(RuntimeStatusRequest.getDefaultInstance());
        }
        @Override
        public ModelSizeResponse size(ModelSizeRequest request) {
            return bstub.withDeadlineAfter(SIZING_TIMEOUT_SECS, SECONDS).modelSize(request);
        }
        @Override
        public PredictModelSizeResponse predictSize(PredictModelSizeRequest request) {
            return bstub.withDeadlineAfter(PREDICT_SIZE_TIMEOUT_MS, MILLISECONDS).predictModelSize(request);
        }
        @Override
        public ListenableFuture<LoadModelResponse> load(LoadModelRequest request) {
            return fstub.loadModel(request);
        }
        @Override
        public ListenableFuture<UnloadModelResponse> unload(UnloadModelRequest request) {
            return fstub.unloadModel(request);
        }
        @Override
        public ManagedChannel getChannel() {
            return (ManagedChannel) bstub.getChannel();
        }
    }

    // looks like "..." in the stacktrace
    private static final StackTraceElement ELLIPSIS_STACK_ELEM = new StackTraceElement(".", ".", null, 0);

    /**
     * Condense the stack trace of exception and causes, for cleaner logs.
     * <p>
     * Only the calling method of the one creating the exception is included,
     * and only the method in which each cause in the chain was created,
     * apart from possibly the root if {@code excludeRoot} is set to {@code true}.
     *
     * @return
     */
    static <T extends Throwable> T trimStack(T t, boolean excludeRoot) {
        for (Throwable next = t, cause; next != null; next = cause) {
            if ((cause = next.getCause()) != null || next == t || !excludeRoot) {
                StackTraceElement[] st = next.getStackTrace();
                // trim to just the *calling* method for the top level exception
                int i = (next == t) ? 1 : 0;
                if (st.length > i) {
                    st = st.length == i + 1 ? new StackTraceElement[] { st[i] }
                            : new StackTraceElement[] { st[i], ELLIPSIS_STACK_ELEM };
                    next.setStackTrace(st);
                }
            }
        }
        return t;
    }

}
