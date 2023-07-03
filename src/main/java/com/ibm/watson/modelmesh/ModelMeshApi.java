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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.ibm.watson.litelinks.NettyCommon;
import com.ibm.watson.litelinks.ThreadContext;
import com.ibm.watson.litelinks.ThreadPoolHelper;
import com.ibm.watson.litelinks.server.ReleaseAfterResponse;
import com.ibm.watson.litelinks.server.ServerRequestThread;
import com.ibm.watson.modelmesh.DataplaneApiConfig.RpcConfig;
import com.ibm.watson.modelmesh.ModelMesh.ExtendedStatusInfo;
import com.ibm.watson.modelmesh.api.DeleteVModelRequest;
import com.ibm.watson.modelmesh.api.DeleteVModelResponse;
import com.ibm.watson.modelmesh.api.EnsureLoadedRequest;
import com.ibm.watson.modelmesh.api.GetStatusRequest;
import com.ibm.watson.modelmesh.api.GetVModelStatusRequest;
import com.ibm.watson.modelmesh.api.ModelMeshGrpc;
import com.ibm.watson.modelmesh.api.ModelStatusInfo;
import com.ibm.watson.modelmesh.api.ModelStatusInfo.ModelCopyInfo;
import com.ibm.watson.modelmesh.api.ModelStatusInfo.ModelStatus;
import com.ibm.watson.modelmesh.api.RegisterModelRequest;
import com.ibm.watson.modelmesh.api.SetVModelRequest;
import com.ibm.watson.modelmesh.api.UnregisterModelRequest;
import com.ibm.watson.modelmesh.api.UnregisterModelResponse;
import com.ibm.watson.modelmesh.api.VModelStatusInfo;
import com.ibm.watson.modelmesh.payload.Payload;
import com.ibm.watson.modelmesh.payload.PayloadProcessor;
import com.ibm.watson.modelmesh.thrift.ApplierException;
import com.ibm.watson.modelmesh.thrift.InvalidInputException;
import com.ibm.watson.modelmesh.thrift.InvalidStateException;
import com.ibm.watson.modelmesh.thrift.ModelInfo;
import com.ibm.watson.modelmesh.thrift.ModelMeshService;
import com.ibm.watson.modelmesh.thrift.ModelNotFoundException;
import com.ibm.watson.modelmesh.thrift.Status;
import com.ibm.watson.modelmesh.thrift.StatusInfo;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Deadline;
import io.grpc.HandlerRegistry;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.InternalNettyServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.ibm.watson.modelmesh.GrpcSupport.*;
import static com.ibm.watson.modelmesh.ModelMeshEnvVars.MM_MULTI_PARALLELISM_ENV_VAR;
import static io.grpc.Status.*;
import static java.lang.System.nanoTime;

/**
 * This class is a simple grpc server which delegates to a thrift
 * {@link ModelMeshService} implementation.
 *
 */
public final class ModelMeshApi extends ModelMeshGrpc.ModelMeshImplBase
        implements ServerCallHandler<ByteBuf, ByteBuf> {

    private static final Logger logger = LoggerFactory.getLogger(ModelMeshApi.class);

    // if requests are coming in via Kubernetes proxying, they may be unbalanced
    public static final boolean UNBALANCED_DEFAULT = true;

    protected static final DataplaneApiConfig dataplaneApiConfig;

    static {
        try {
            dataplaneApiConfig = DataplaneApiConfig.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // max number of model invocations to run in parallel per multi-model request
    public final int multiParallelism;

    protected final SidecarModelMesh delegate;

    protected final VModelManager vmm;

    protected final ServerServiceDefinition thisService;
    protected final Server server;

    protected final ThreadPoolExecutor threads;
    protected final EventLoopGroup bossGroup;

    // null if header logging is not enabled.  
    protected final LogRequestHeaders logHeaders;

    private final PayloadProcessor payloadProcessor;

    private final ThreadLocal<long[]> localIdCounter = ThreadLocal.withInitial(() -> new long[1]);

    /**
     * Create <b>and start</b> the server.
     *
     * @param delegate
     * @param vmm
     * @param port
     * @param keyCert               null for no TLS
     * @param privateKey            null for no TLS
     * @param privateKeyPassphrase
     * @param clientAuth
     * @param trustCerts
     * @param maxMessageSize        in bytes
     * @param maxConnectionAge      in seconds
     * @param maxConnectionAgeGrace in seconds, custom grace time for graceful connection termination
     * @param logHeaders
     * @param payloadProcessor      a processor of payloads
     * @throws IOException
     */
    public ModelMeshApi(SidecarModelMesh delegate, VModelManager vmm, int port, File keyCert, File privateKey,
            String privateKeyPassphrase, ClientAuth clientAuth, File[] trustCerts,
            int maxMessageSize, int maxHeadersSize, long maxConnectionAge, long maxConnectionAgeGrace,
            LogRequestHeaders logHeaders, PayloadProcessor payloadProcessor) throws IOException {

        this.delegate = delegate;
        this.vmm = vmm;
        this.logHeaders = logHeaders;
        this.payloadProcessor = payloadProcessor;

        this.multiParallelism = getMultiParallelism();

        this.thisService = bindService();

        // This thread pool is used for incoming external gRPC request threads only
        // todo make core/max counts configurable
        this.threads = ThreadPoolHelper.newThreadPool(8, 64, 30L, TimeUnit.MINUTES,
                new ThreadFactoryBuilder().setNameFormat("mmesh-req-thread-%d")
                        .setThreadFactory(r -> new ServerRequestThread(r) {
                            @Override
                            public boolean logInterrupt(long runTime) {
                                return super.logInterrupt(runTime) && !threads.isShutdown();
                            }
                        }).build()); // daemon tbd

        boolean ok = false;
        try {
            ServerServiceDefinition service = logHeaders == null ?
                ServerInterceptors.intercept(thisService, BALANCED_HEADER_PASSER) :
                ServerInterceptors.intercept(thisService, logHeaders.serverInterceptor(), BALANCED_HEADER_PASSER);

            NettyServerBuilder nsb = NettyServerBuilder.forPort(port).executor(threads)
                    .maxInboundMessageSize(maxMessageSize)
                    .addService(service).fallbackHandlerRegistry(new Registry());

            if (maxHeadersSize >= 0) {
                nsb.maxInboundMetadataSize(maxHeadersSize);
            }

            EventLoopGroup workerGroup = NettyCommon.getWorkerGroup();

            ThreadFactory tfac = new ThreadFactoryBuilder().setDaemon(true).setThreadFactory(FastThreadLocalThread::new)
                    .setNameFormat("mm-boss-elg-thread").build();

            if (workerGroup instanceof EpollEventLoopGroup) {
                bossGroup = new EpollEventLoopGroup(1, tfac);
                nsb.channelType(EpollServerSocketChannel.class);
                logger.info("Using epoll transport for gRPC server");
            } else {
                bossGroup = new NioEventLoopGroup(1, tfac);
                nsb.channelType(NioServerSocketChannel.class);
                logger.warn("Epoll transport not available for gRPC server");
            }

            nsb.bossEventLoopGroup(bossGroup).workerEventLoopGroup(workerGroup);
            if (privateKey != null) {
                nsb.sslContext(createSSLContext(keyCert, privateKey, privateKeyPassphrase, clientAuth, trustCerts));
            }

            if (maxConnectionAge < Long.MAX_VALUE) {
                nsb.maxConnectionAge(maxConnectionAge, TimeUnit.SECONDS);
            }

            if (maxConnectionAgeGrace < Long.MAX_VALUE) {
                nsb.maxConnectionAgeGrace(maxConnectionAgeGrace, TimeUnit.SECONDS);
            }

            //TODO later make these configurable probably
            InternalNettyServerBuilder.setStatsEnabled(nsb, false);
            InternalNettyServerBuilder.setTracingEnabled(nsb, false);

            this.server = nsb.build();
            ok = true;
        } finally {
            if (!ok) {
                threads.shutdownNow();
                shutdownEventLoops();
            }
        }
    }

    public void start() throws IOException {
        server.start();
    }

    private static SslContext createSSLContext(File caCert, File privateKey, String password,
            ClientAuth clientAuth, File... trustCerts) throws IOException {
        InputStream trustCertsStream = null;
        if (trustCerts != null) {
            List<InputStream> trustCertList = new ArrayList<>();
            for (File f : trustCerts) {
                trustCertList.add(new FileInputStream(f));
            }
            trustCertsStream = new SequenceInputStream(Collections.enumeration(trustCertList));
        }
        try (InputStream caCertStream = new FileInputStream(caCert);
             InputStream privateKeyStream = new FileInputStream(privateKey)) {
            return GrpcSslContexts.forServer(caCertStream, privateKeyStream, password)
                    // If trustCertsStream is null the default system/OS certificates will be used
                    .trustManager(trustCertsStream).clientAuth(clientAuth != null ? clientAuth : ClientAuth.NONE)
                    .build();
        }
    }

    ExecutorService getRequestThreads() {
        return threads;
    }

    protected void adjustThreadCount(int delta) {
        if (delta != 0) {
            synchronized (threads) {
                threads.setMaximumPoolSize(threads.getMaximumPoolSize() + delta);
            }
        }
    }

    public void shutdown(long timeout, TimeUnit unit) throws InterruptedException {
        boolean done = timeout > 0 && server.shutdown().awaitTermination(timeout, unit);
        if (!done) {
            server.shutdownNow();
        }
        if (payloadProcessor != null) {
            try {
                payloadProcessor.close();
            } catch (IOException e) {
                logger.warn("Error closing PayloadProcessor {}: {}", payloadProcessor, e.getMessage());
            }
        }
        threads.shutdownNow();
        shutdownEventLoops();
    }

    protected void shutdownEventLoops() {
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
    }

    static final Context.Key<String> BALANCED_CTX_KEY = Context.key(BALANCED_META_KEY.name());

    /**
     * Used to pass the mm-balanced metadata header to model management methods
     */
    static final ServerInterceptor BALANCED_HEADER_PASSER = new ServerInterceptor() {
        @Override
        public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {
            String balanced = headers.get(BALANCED_META_KEY);
            return balanced == null ? next.startCall(call, headers)
                    : Contexts.interceptCall(Context.current().withValue(BALANCED_CTX_KEY, balanced),
                    call, headers, next);
        }
    };

    protected static void setUnbalancedLitelinksContextParam(String grpcBalancedHeader) {
        if (grpcBalancedHeader != null ? !"true".equals(grpcBalancedHeader) : UNBALANCED_DEFAULT) {
            setUnbalancedLitelinksContextParam();
        }
    }

    protected static void setUnbalancedLitelinksContextParam() {
        ThreadContext.addContextEntry(ModelMesh.UNBALANCED_KEY, "true"); // unbalanced
    }

    protected static void setvModelIdLiteLinksContextParam(String vModelId) {
        ThreadContext.addContextEntry(ModelMesh.VMODELID, vModelId);
    }

    // ----------------- concrete model management methods

    @Override
    public void registerModel(RegisterModelRequest request, StreamObserver<ModelStatusInfo> response) {
        String modelId = request.getModelId();
        if (!validateModelId(modelId, response)) {
            return;
        }
        ModelInfo modelInfo = new ModelInfo();
        com.ibm.watson.modelmesh.api.ModelInfo grpcModelInfo = request.getModelInfo();
        if (grpcModelInfo != null) {
            modelInfo.setServiceType(grpcModelInfo.getType());
            modelInfo.setModelPath(grpcModelInfo.getPath());
            modelInfo.setEncKey(grpcModelInfo.getKey());
        }
        boolean loadNow = request.getLoadNow();
        InterruptingListener cancelListener = null;
        try {
            if (loadNow) {
                setUnbalancedLitelinksContextParam(BALANCED_CTX_KEY.get());
                if (request.getSync()) {
                    cancelListener = newInterruptingListener();
                }
            }
            StatusInfo si = delegate.registerModel(modelId, modelInfo, loadNow, request.getSync(),
                    request.getLastUsedTime());
            respondAndComplete(response, convertStatusInfo(si));
        } catch (Exception e) {
            handleException(e, response);
        } finally {
            closeQuietly(cancelListener);
            clearThreadLocals();
        }
    }

    @Override
    public void unregisterModel(UnregisterModelRequest request, StreamObserver<UnregisterModelResponse> response) {
        String modelId = request.getModelId();
        if (!validateModelId(modelId, response)) return;
        try {
            delegate.deleteModel(modelId);
            respondAndComplete(response, UnregisterModelResponse.getDefaultInstance());
        } catch (Exception e) {
            handleException(e, response);
        }
    }

    @Override
    public void getModelStatus(GetStatusRequest request, StreamObserver<ModelStatusInfo> response) {
        String modelId = request.getModelId();
        if (!validateModelId(modelId, response)) {
            return;
        }
        try (InterruptingListener cancelListener = newInterruptingListener()) {
            StatusInfo si = delegate.getStatus(modelId);
            respondAndComplete(response, convertStatusInfo(si));
        } catch (Exception e) {
            handleException(e, response);
        } finally {
            clearThreadLocals();
        }
    }

    @Override
    public void ensureLoaded(EnsureLoadedRequest request, StreamObserver<ModelStatusInfo> response) {
        String modelId = request.getModelId();
        if (!validateModelId(modelId, response)) {
            return;
        }
        boolean sync = request.getSync();
        InterruptingListener cancelListener = sync ? newInterruptingListener() : null;
        try {
            setUnbalancedLitelinksContextParam(BALANCED_CTX_KEY.get());
            StatusInfo si = delegate.ensureLoaded(modelId, request.getLastUsedTime(), null, sync, true);
            respondAndComplete(response, convertStatusInfo(si));
        } catch (Exception e) {
            handleException(e, response);
        } finally {
            closeQuietly(cancelListener);
            clearThreadLocals();
        }
    }

    // Returned ModelResponse will be released once the request thread exits so
    // must be retained before transferring.
    // non-private to avoid synthetic method access
    ModelResponse callModel(String originalModelId, boolean isVModel, String methodName, String grpcBalancedHeader,
            Metadata headers, ByteBuf data) throws Exception {
        boolean unbalanced = grpcBalancedHeader == null ? UNBALANCED_DEFAULT : !"true".equals(grpcBalancedHeader);
        if (!isVModel) {
            if (unbalanced) {
                setUnbalancedLitelinksContextParam();
            }
            return delegate.callModel(originalModelId, methodName, headers, data);
        }
        String vModelId = originalModelId;
        if (delegate.metrics.isEnabled()) {
            setvModelIdLiteLinksContextParam(originalModelId);
        }
        boolean first = true;
        while (true) {
            String modelId = vmm().resolveVModelId(vModelId, originalModelId);
            if (unbalanced) {
                setUnbalancedLitelinksContextParam();
            }
            try {
                return delegate.callModel(modelId, methodName, headers, data);
            } catch (ModelNotFoundException mnfe) {
                if (!first) throw mnfe;
            } catch (Exception e) {
                logger.error("Exception invoking " + methodName + " method of resolved model " + modelId + " of vmodel "
                             + vModelId + ": " + e.getClass().getSimpleName() + ": " + e.getMessage());
                throw e;
            }
            // try again
            first = false;
            data.readerIndex(0); // rewind buffer
        }
    }

    // -----

    protected static ModelStatusInfo convertStatusInfo(StatusInfo si) {
        ModelStatusInfo.Builder bld = ModelStatusInfo.newBuilder();
        if (si.isSetErrorMessages()) {
            bld.addAllErrors(si.getErrorMessages());
        }
        if (si instanceof ExtendedStatusInfo) {
            ExtendedStatusInfo fsi = (ExtendedStatusInfo) si;
            if (fsi.copiesInfo != null && fsi.copiesInfo.length != 0) {
                bld.addAllModelCopyInfos(() -> Stream.of(fsi.copiesInfo).map(
                        ci -> ModelCopyInfo.newBuilder().setCopyStatus(convertStatus(ci.status))
                                .setLocation(ci.location).setTime(ci.time).build()).iterator());
            }
        }
        return bld.setStatus(convertStatus(si.getStatus())).build();
    }

    protected static ModelStatus convertStatus(Status s) {
        switch (s) {
        case LOADED:
            return ModelStatus.LOADED;
        case LOADING:
            return ModelStatus.LOADING;
        case LOADING_FAILED:
            return ModelStatus.LOADING_FAILED;
        case NOT_FOUND:
            return ModelStatus.NOT_FOUND;
        case NOT_LOADED:
            return ModelStatus.NOT_LOADED;
        case NOT_CHECKED:
        default:
            return ModelStatus.UNKNOWN;
        }
    }

    static final io.grpc.Status MODEL_NOT_FOUND_STATUS = NOT_FOUND.withDescription("model not found");

    static final io.grpc.Status VMODEL_NOT_FOUND_STATUS = NOT_FOUND.withDescription("vmodel not found");

    static final StatusException VMODEL_NOT_FOUND_STATUS_E = VMODEL_NOT_FOUND_STATUS.asException();

    private static final StatusException INVALID_MODEL_ID = INVALID_ARGUMENT
            .withDescription("must provide non-empty modelId").asException();

    static {
        // clear stacktraces from constant exceptions
        StackTraceElement[] empty = new StackTraceElement[0];
        VMODEL_NOT_FOUND_STATUS_E.setStackTrace(empty);
        INVALID_MODEL_ID.setStackTrace(empty);
    }

    protected static final Listener<?> EMPTY_LISTENER = new Listener<Object>() {
    };

    @SuppressWarnings("unchecked")
    protected static <ReqT> Listener<ReqT> closeCall(ServerCall<ReqT, ?> call, io.grpc.Status status,
            String description) {
        call.close(status.withDescription(description), emptyMeta());
        return (Listener<ReqT>) EMPTY_LISTENER;
    }

    protected static void clearThreadLocals() {
        ThreadContext.removeCurrentContext();
        MDC.clear();
    }

    protected static void handleException(Exception e, StreamObserver<?> response) {
        StatusException se = new StatusException(toStatus(e));
        // trim to just the calling method
        se.setStackTrace(new StackTraceElement[] { se.getStackTrace()[1] });
        response.onError(se);
    }

    protected static <RespT> void respondAndComplete(StreamObserver<RespT> response, RespT message) {
        response.onNext(message);
        response.onCompleted();
    }

    protected static io.grpc.Status toStatus(Exception e) {
        io.grpc.Status s = null;
        String msg = e.getMessage();
        if (e instanceof ModelNotFoundException) {
            return MODEL_NOT_FOUND_STATUS;
        } else if (e instanceof InvalidInputException) {
            s = msg != null && msg.startsWith("Model already exists with different") ? ALREADY_EXISTS
                    : INVALID_ARGUMENT;
        } else if (e instanceof ApplierException) {
            String codeString = ((ApplierException) e).getGrpcStatusCode();
            Code code = null;
            if (codeString != null) {
                try {
                    code = Code.valueOf(codeString);
                } catch (IllegalStateException ise) {}
            }
            //TODO *maybe* need to filter some codes here
            s = code != null ? fromCode(code) : INTERNAL;
        } else if (e instanceof InvalidStateException) {
            s = FAILED_PRECONDITION;
        } else if (ModelMesh.isInterruption(e)) {
            Deadline deadline = Context.current().getDeadline();
            return deadline != null && deadline.isExpired() ? DEADLINE_EXCEEDED.withCause(e)
                    : CANCELLED.withCause(e);
        } else if (ModelMesh.isTimeout(e)) {
            return DEADLINE_EXCEEDED.withCause(e);
        } else if (e instanceof TException) {
            s = INTERNAL;
        } else {
            s = fromThrowable(e);
            return s.getDescription() != null ? s
                    : s.withDescription(e.getClass().getName() + (msg != null ? ": " + msg : ""));
        }
        return s.withCause(e).withDescription(msg);
    }

    protected static boolean validateModelId(String modelId, StreamObserver<?> response) {
        if (modelId != null && !modelId.isEmpty()) {
            return true;
        }
        response.onError(INVALID_MODEL_ID);
        return false;
    }

    protected static Iterable<String> getAll(Metadata headers, Metadata.Key<String> key1, Metadata.Key<String> key2) {
        Iterable<String> it1 = headers.getAll(key1), it2 = headers.getAll(key2);
        return it1 == null ^ it2 == null ? it1 == null ? it2 : it1 : it1 == null ? null : Iterables.concat(it1, it2);
    }

    static final class ModelIds {
        static final ModelIds DEFAULT = new ModelIds(null, false, null);

        final Iterable<String> modelIds;
        final boolean isVmodel;
        final io.grpc.Status errorStatus;

        ModelIds(Iterable<String> modelIds, boolean isVmodel, io.grpc.Status errorStatus) {
            this.modelIds = modelIds;
            this.isVmodel = isVmodel;
            this.errorStatus = errorStatus;
        }
    }

    /**
     * Attempt to extract model ids or vmodel ids from call headers
     */
    protected static ModelIds getModelIdsFromHeaders(Metadata headers) {
        Iterable<String> mids = headers.getAll(MODEL_ID_HEADER_KEY);
        if (mids == null) mids = headers.getAll(MODEL_ID_BIN_HEADER_KEY);
        Iterable<String> vmids = headers.getAll(VMODEL_ID_HEADER_KEY);
        if (vmids == null) vmids = headers.getAll(VMODEL_ID_BIN_HEADER_KEY);
        if (vmids == null) {
            if (mids == null) {
                mids = headers.getAll(MODEL_ID_HEADER_KEY_OLD);
                if (mids == null) {
                    return new ModelIds(null, false,
                            INVALID_ARGUMENT.withDescription(
                                    "must include " + MODEL_ID_HEADER_KEY.name() + " header"));
                }
            }
            return new ModelIds(mids, false, null);
        } else if (mids != null) {
            return new ModelIds(null, false,
                    INVALID_ARGUMENT.withDescription("must include only one of model id and vmodel id"));
        } else {
            return new ModelIds(vmids, true, null);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Listener<ByteBuf> startCall(ServerCall<ByteBuf, ByteBuf> call, Metadata headers) {
        long startNanos = nanoTime();
        final String methodName = call.getMethodDescriptor().getFullMethodName();
        final RpcConfig rpcConfig = dataplaneApiConfig.getConfig().getRpcConfig(methodName);
        if (rpcConfig == null) {
            call.close(UNIMPLEMENTED, emptyMeta()); // RPC method not allowed
            return (Listener<ByteBuf>) EMPTY_LISTENER;
        }
        final ModelIds mids;
        if (!rpcConfig.shouldExtract()) {
            // When not doing id extraction, try to resolve model ids from headers
            // so that we can fail-fast if necessary.
            mids = getModelIdsFromHeaders(headers);
            if (mids.errorStatus != null) {
                call.close(mids.errorStatus, emptyMeta());
                return (Listener<ByteBuf>) EMPTY_LISTENER;
            }
        } else {
            mids = ModelIds.DEFAULT; // not yet determined
        }

        call.request(2); // request 2 to force failure if streaming method

        return new Listener<ByteBuf>() {
            ByteBuf reqMessage;
            boolean canInvoke = true;
            Iterable<String> modelIds = mids.modelIds;
            boolean isVModel = mids.isVmodel;

            @Override
            public void onMessage(ByteBuf message) {
                if (this.reqMessage != null) {
                    // Safe to close the call, because the application has not yet been invoked
                    call.close(UNIMPLEMENTED.withDescription("Streaming not supported yet"), emptyMeta());
                    canInvoke = false;
                } else {
                    this.reqMessage = message;

                    if (modelIds == null) {
                        // First try to get id from the message
                        String modelId = ProtoSplicer.extractId(message, rpcConfig.getIdExtractionPath());
                        if (modelId != null) {
                            modelIds = Collections.singleton(modelId);
                            isVModel = rpcConfig.isvModelId();
                        } else {
                            // If target field is not set, fall back to looking for headers
                            ModelIds mids = getModelIdsFromHeaders(headers);
                            if (mids.errorStatus != null) {
                                call.close(mids.errorStatus, emptyMeta());
                                canInvoke = false;
                            }
                            modelIds = mids.modelIds;
                            isVModel = mids.isVmodel;
                        }
                    }
                    // delay invoking until onHalfClose() to ensure the client half-closes
                }
            }

            @Override
            public void onHalfClose() {
                if (!canInvoke) {
                    return;
                }
                if (reqMessage == null) {
                    // Safe to close the call, because the application has not yet been invoked
                    call.close(INTERNAL.withDescription("Half-closed without a request"), emptyMeta());
                    return;
                }
                int reqReaderIndex = reqMessage.readerIndex();
                int reqSize = reqMessage.readableBytes();
                int respSize = -1;
                int respReaderIndex = 0;

                io.grpc.Status status = INTERNAL;
                String modelId = null;
                String requestId = null;
                ModelResponse response = null;
                try (InterruptingListener cancelListener = newInterruptingListener()) {
                    if (logHeaders != null) {
                        logHeaders.addToMDC(headers); // MDC cleared in finally block
                    }
                    if (payloadProcessor != null) {
                        requestId = Thread.currentThread().getId() + "-" + ++localIdCounter.get()[0];
                    }
                    try {
                        String balancedMetaVal = headers.get(BALANCED_META_KEY);
                        Iterator<String> midIt = modelIds.iterator();
                        // guaranteed at least one
                        modelId = validateModelId(midIt.next(), isVModel);
                        if (!midIt.hasNext()) {
                            // single model case (most common)
                            response = callModel(modelId, isVModel, methodName,
                                    balancedMetaVal, headers, reqMessage).retain();
                        } else {
                            // multi-model case (specialized)
                            boolean allRequired = "all".equalsIgnoreCase(headers.get(REQUIRED_KEY));
                            List<String> idList = new ArrayList<>();
                            idList.add(modelId);
                            while (midIt.hasNext()) {
                                idList.add(validateModelId(midIt.next(), isVModel));
                            }
                            response = applyParallelMultiModel(idList, isVModel, methodName,
                                    balancedMetaVal, headers, reqMessage, allRequired);
                        }
                    } finally {
                        if (payloadProcessor != null) {
                            processPayload(reqMessage.readerIndex(reqReaderIndex),
                                    requestId, modelId, methodName, headers, null, true);
                        } else {
                            releaseReqMessage();
                        }
                        reqMessage = null; // ownership released or transferred
                    }

                    respReaderIndex = response.data.readerIndex();
                    respSize = response.data.readableBytes();
                    call.sendHeaders(response.metadata);
                    call.sendMessage(response.data);
                    // response is released via ReleaseAfterResponse.releaseAll()
                    status = OK;
                } catch (Exception e) {
                    status = toStatus(e);
                    Code code = status.getCode();
                    if (code != Code.INTERNAL && code != Code.UNKNOWN) {
                        logger.error("Invocation of method " + methodName + " failed for " + (isVModel ? "v" : "")
                                     + "model ids " + Iterables.toString(modelIds) + ": "
                                     + toShortString(status));
                    } else {
                        logger.error("Exception invoking " + methodName + " method for " + (isVModel ? "v" : "")
                                     + "model ids " + Iterables.toString(modelIds), e);
                    }
                    if (code == Code.UNIMPLEMENTED) {
                        evictMethodDescriptor(methodName);
                    }
                } finally {
                    final boolean releaseResponse = status != OK;
                    if (payloadProcessor != null) {
                        ByteBuf data = null;
                        Metadata metadata = null;
                        if (response != null) {
                            data = response.data.readerIndex(respReaderIndex);
                            metadata = response.metadata;
                        }
                        processPayload(data, requestId, modelId, methodName, metadata, status, releaseResponse);
                    } else if (releaseResponse && response != null) {
                        response.release();
                    }
                    ReleaseAfterResponse.releaseAll();
                    clearThreadLocals();
                    //TODO(maybe) additional trailer info in exception case?
                    call.close(status, emptyMeta());
                    Metrics metrics = delegate.metrics;
                    if (metrics.isEnabled()) {
                        Iterator<String> midIt = modelIds.iterator();
                        while (midIt.hasNext()) {
                            if (isVModel) {
                                String mId = null;
                                String vmId = midIt.next();
                                try {
                                    mId = vmm().resolveVModelId(midIt.next(), mId);
                                    metrics.logRequestMetrics(true, methodName, nanoTime() - startNanos,
                                    status.getCode(), reqSize, respSize, mId, vmId);
                                }
                                catch (Exception e) {
                                    logger.error("Could not resolve model id for vModelId" + vmId, e);
                                    metrics.logRequestMetrics(true, methodName, nanoTime() - startNanos,
                                    status.getCode(), reqSize, respSize, "", vmId);
                                }
                            } else {
                                metrics.logRequestMetrics(true, methodName, nanoTime() - startNanos,
                                    status.getCode(), reqSize, respSize, midIt.next(), "");
                            }
                        }
                    }
                }
            }

            /**
             * Invoke PayloadProcessor on the request/response data
             * @param data the binary data
             * @param payloadId the id of the request
             * @param modelId the id of the model
             * @param methodName the name of the invoked method
             * @param metadata the method name metadata
             * @param status null for requests, non-null for responses
             * @param takeOwnership whether the processor should take ownership
             */
            private void processPayload(ByteBuf data, String payloadId, String modelId, String methodName,
                                        Metadata metadata, io.grpc.Status status, boolean takeOwnership) {
                Payload payload = null;
                try {
                    assert payloadProcessor != null;
                    if (!takeOwnership) {
                        ReferenceCountUtil.retain(data);
                    }
                    payload = new Payload(payloadId, modelId, methodName, metadata, data, status);
                    if (payloadProcessor.process(payload)) {
                        data = null; // ownership transferred
                    }
                } catch (Throwable t) {
                    logger.warn("Error while processing payload: {}", payload, t);
                } finally {
                    ReferenceCountUtil.release(data);
                }
            }

            @Override
            public void onComplete() {
                releaseReqMessage();
            }

            private void releaseReqMessage() {
                if (reqMessage == null) {
                    return;
                }
                reqMessage.release();
                reqMessage = null;
            }
        };
    }

    // throws StatusRuntimeException
    static String validateModelId(String modelId, boolean isVModel) {
        if (!Strings.isNullOrEmpty(modelId)) {
            return modelId;
        }
        throw statusException(INVALID_ARGUMENT,
                "provided " + (isVModel ? "vmodel" : "model") + "-id header must be non-empty");
    }

    static final class ModelResponse {
        final Metadata metadata;
        final ByteBuf data;

        public ModelResponse(@Nullable Metadata metadata, ByteBuf data) {
            this.metadata = metadata;
            this.data = Preconditions.checkNotNull(data, "buffer");
        }

        public ModelResponse retain() {
            data.retain();
            return this;
        }

        public void release() {
            data.release();
        }

        public void releaseAfterResponse() {
            ReleaseAfterResponse.addReleasable(data);
        }
    }

    // used to ensure that transferred releaseable will always be released,
    // regardless of outcome (success/failure/cancellation/interruption,...)
    static class ParallelFuture extends AbstractFuture<AutoCloseable> {
        Future<ModelResponse> taskFuture;

        public void transfer(AutoCloseable toRelease) {
            if (!set(toRelease)) {
                closeQuietly(toRelease);
            }
        }

        public void cancel() {
            releaseOnTransfer();
            taskFuture.cancel(true);
        }

        // called only when complete and !failed
        public ModelResponse getResponse() throws Exception {
            return taskFuture.get();
        }

        // called only when complete
        public void releaseAfterResponse() {
            if (!taskFuture.isDone()) {
                throw new IllegalStateException();
            }
            try {
                taskFuture.get();
                AutoCloseable toRelease = getIfDone();
                if (toRelease != null) {
                    ReleaseAfterResponse.addCloseable(toRelease);
                }
            } catch (Exception e) {
                releaseOnTransfer();
            }
        }

        private void releaseOnTransfer() {
            addListener(() -> closeQuietly(getIfDone()), MoreExecutors.directExecutor());
        }

        private AutoCloseable getIfDone() {
            try {
                return isDone() ? Uninterruptibles.getUninterruptibly(this) : null;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    // returned ModelResponse *must* be released (caller receives ownership)
    protected ModelResponse applyParallelMultiModel(List<String> modelIds, boolean isVModel, String methodName,
            String balancedMetaVal, Metadata headers, ByteBuf data, boolean requireAll) throws Exception {

        final CompletionService<ModelResponse> parallel = new ExecutorCompletionService<>(threads);
        final int total = modelIds.size();
        assert total > 0;
        int sent = 0, done = 0, found = 0;
        List<ParallelFuture> responseFutures = new ArrayList<>(total);

        //TODO maybe simplify multiParallelism == 1 case (stay in same thread)

        //TODO later convert to fully async (no CompletionService)

        final Context curContext = Context.current();
        adjustThreadCount(1);
        try {
            while (done < total) {
                while (sent < Math.min(total, done + multiParallelism)) {
                    final int index = sent;
                    final String modelId = modelIds.get(index);
                    final ParallelFuture ourFuture = new ParallelFuture();
                    ourFuture.taskFuture = parallel.submit(() -> {
                        // use attach directly to avoid creating extra Callable wrappers
                        Context prevContext = curContext.attach();
                        try {
                            if (logHeaders != null) {
                                logHeaders.addToMDC(headers);
                            }
                            // need to pass slices of the buffers for threadsafety
                            return callModel(modelId, isVModel, methodName, balancedMetaVal, headers, data.slice());
                        } catch (ModelNotFoundException mnfe) {
                            logger.warn("model " + modelId + " not found (from supplied list of " + total + ")");
                            if (!requireAll) {
                                return null;
                            }
                            throw mnfe;
                        } catch (StatusException | StatusRuntimeException se) {
                            if (Code.NOT_FOUND == fromThrowable(se).getCode()) {
                                logger.warn((isVModel ? "vmodel " : "model ") + modelId
                                            + " not found (from supplied list of " + total + ")");
                                if (!requireAll) {
                                    return null;
                                }
                            }
                            throw se;
                        } finally {
                            ourFuture.transfer(ReleaseAfterResponse.takeOwnership());
                            curContext.detach(prevContext);
                            clearThreadLocals();
                        }
                    });
                    responseFutures.add(ourFuture);
                    sent++;
                }

                // will throw if individual task fails
                ModelResponse mr = parallel.take().get();
                if (mr != null) {
                    found++;
                }
                // else was not found - must be !requireAll (OK)

                done++;
            }
        } catch (InterruptedException | RuntimeException e) {
            cancelAll(responseFutures);
            throw e;
        } catch (ExecutionException e) {
            cancelAll(responseFutures);
            Throwables.throwIfUnchecked(e.getCause());
            Throwables.throwIfInstanceOf(e.getCause(), Exception.class);
            throw new RuntimeException(e.getCause());
        } finally {
            adjustThreadCount(-1);
        }

        // have this thread inherit ownership of all releaseables
        responseFutures.forEach(ParallelFuture::releaseAfterResponse);

        if (found == 0) {
            throw statusException(NOT_FOUND, "none of the requested models were found");
        }

        // this is done at the end to ensure concatenation ordering
        // matches the order of model id headers received
        CompositeByteBuf concatBuf = found > 1 ? Unpooled.compositeBuffer(found) : null;
        try {
            Metadata mergedMeta = null;
            for (ParallelFuture f : responseFutures) {
                ModelResponse mr = f.getResponse(); // certain to be done and !failed here
                if (mr == null) {
                    continue;
                }
                if (concatBuf == null) {
                    return mr.retain(); // found == 1 case
                }
                concatBuf.addComponent(true, mr.data.retain());
                if (mergedMeta == null) {
                    mergedMeta = mr.metadata;
                } else {
                    mergedMeta.merge(mr.metadata);
                }
            }
            // found > 1
            ModelResponse mr = new ModelResponse(mergedMeta, concatBuf);
            concatBuf = null;
            return mr;
        } finally {
            ReferenceCountUtil.release(concatBuf); // ensure no leak in case of unexpected exception
        }
    }

    protected static int getMultiParallelism() {
        String mpEnv = System.getenv(MM_MULTI_PARALLELISM_ENV_VAR);
        if (mpEnv == null) {
            return 4; // default is 4
        }
        try {
            int mp = Integer.parseInt(mpEnv);
            if (mp < 1) {
                throw new IllegalArgumentException(MM_MULTI_PARALLELISM_ENV_VAR + " env var must be >1: " + mpEnv);
            }
            return mp;
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException(
                    "invalid value of " + MM_MULTI_PARALLELISM_ENV_VAR + " env var: " + mpEnv);
        }
    }

    protected static <T> void cancelAll(Iterable<ParallelFuture> futs) {
        if (futs != null) for (ParallelFuture f : futs) f.cancel();
    }

    //TODO maybe move to utility
    static void closeQuietly(AutoCloseable close) {
        if (close != null) {
            try {
                close.close();
            } catch (Exception e) {}
        }
    }

    static StatusRuntimeException statusException(io.grpc.Status status, String description) {
        throw status.withDescription(description).asRuntimeException();
    }

    // this just for backwards compatibility (deprecated)
    private static final String OLD_TAS_SN_PFX = "com.ibm.watson.tas.proto.TasRuntime/";
    private static final int OLD_TAS_SN_LEN = OLD_TAS_SN_PFX.length() - 1;

    private static final String ADD_MODEL_SFX = "/addModel", DELETE_MODEL_SFX = "/deleteModel";

    class Registry extends HandlerRegistry {
        final String serviceName = thisService.getServiceDescriptor().getName();
        final String serviceNamePfx = serviceName + "/";

        private final ServerMethodDefinition<?, ?> REG_MODEL_METH_DEF = thisService
                .getMethod(ModelMeshGrpc.getRegisterModelMethod().getFullMethodName());
        private final ServerMethodDefinition<?, ?> UNREG_MODEL_METH_DEF = thisService
                .getMethod(ModelMeshGrpc.getUnregisterModelMethod().getFullMethodName());

        @Override
        public ServerMethodDefinition<?, ?> lookupMethod(String methodName, String authority) {
            // backwards compatibility for prior register/unregister method names (add/delete)
            if (methodName.startsWith(serviceNamePfx)) {
                ServerMethodDefinition<?, ?> converted = convertOldAddDelete(methodName);
                if (converted != null) {
                    return converted;
                }
            }
            // convert requests to legacy TasRuntime grpc service
            else if (methodName.startsWith(OLD_TAS_SN_PFX)) {
                ServerMethodDefinition<?, ?> converted = convertOldAddDelete(methodName);
                return converted != null ? converted
                        : thisService.getMethod(serviceName + methodName.substring(OLD_TAS_SN_LEN));
            }
            return getMethodDefiniton(methodName);
        }

        // returns null if not applicable
        private ServerMethodDefinition<?, ?> convertOldAddDelete(String methodName) {
            if (methodName.endsWith(ADD_MODEL_SFX)) {
                return REG_MODEL_METH_DEF;
            } else if (methodName.endsWith(DELETE_MODEL_SFX)) {
                return UNREG_MODEL_METH_DEF;
            }
            return null;
        }

        //TODO add mechanism to prevent map filling up with UNIMPLEMENTED methods

        // copy-on-write map
        private final AtomicReference<Map<String, ServerMethodDefinition<ByteBuf, ByteBuf>>> definitions
                = new AtomicReference<>(Collections.emptyMap());

        private ServerMethodDefinition<ByteBuf, ByteBuf> getMethodDefiniton(String methodName) {
            ServerMethodDefinition<ByteBuf, ByteBuf> newMd = null;
            while (true) {
                Map<String, ServerMethodDefinition<ByteBuf, ByteBuf>> current = definitions.get();
                ServerMethodDefinition<ByteBuf, ByteBuf> md = current.get(methodName);
                if (md != null) {
                    return md;
                }
                if (newMd == null) {
                    newMd = ServerMethodDefinition.create(getMethodDescriptor(methodName), ModelMeshApi.this);
                }
                Map<String, ServerMethodDefinition<ByteBuf, ByteBuf>> copy = new TreeMap<>(current);
                copy.put(methodName, newMd);
                if (definitions.compareAndSet(current, copy)) {
                    return newMd;
                }
            }
        }
    }

    // ----------- vmodel API - logic in VModelManager class

    private VModelManager vmm() {
        if (vmm != null) {
            return vmm;
        }
        throw UNIMPLEMENTED.withDescription("vmodels not supported").asRuntimeException();
    }

    @Override
    public void setVModel(SetVModelRequest request, StreamObserver<VModelStatusInfo> response) {
        try {
            respondAndComplete(response, vmm().updateVModel(request));
        } catch (Throwable t) {
            response.onError(t);
            io.grpc.Status status = fromThrowable(t);
            Code code = status.getCode();
            if (logStackTrace(code)) {
                logger.warn("setVModel returning error", t);
            } else {
                logger.warn("setVModel returning " + toShortString(status));
            }
            Throwables.throwIfInstanceOf(t, Error.class);
        } finally {
            clearThreadLocals();
        }
    }

    @Override
    public void getVModelStatus(GetVModelStatusRequest request, StreamObserver<VModelStatusInfo> response) {
        try {
            respondAndComplete(response, vmm().getVModelStatus(request.getVModelId(), request.getOwner()));
        } catch (Throwable t) {
            response.onError(t);
            logger.warn("getVModelStatus returning error", t);
            Throwables.throwIfInstanceOf(t, Error.class);
        } finally {
            clearThreadLocals();
        }
    }

    @Override
    public void deleteVModel(DeleteVModelRequest request, StreamObserver<DeleteVModelResponse> response) {
        try {
            vmm().deleteVModel(request.getVModelId(), request.getOwner());
            respondAndComplete(response, DeleteVModelResponse.getDefaultInstance());
        } catch (Throwable t) {
            response.onError(t);
            logger.warn("deleteVModel returning error", t);
            Throwables.throwIfInstanceOf(t, Error.class);
        }
    }

    protected static boolean logStackTrace(Code code) {
        return code != Code.INVALID_ARGUMENT && code != Code.NOT_FOUND && code != Code.ALREADY_EXISTS
               && code != Code.FAILED_PRECONDITION;
    }
}
