package com.ibm.watson.tas.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 * <pre>
 * this is a grpc version of the external tas-runtime interface
 * for managing and serving models
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.9.1)",
    comments = "Source: tas-runtime.proto")
public final class TasRuntimeGrpc {

  private TasRuntimeGrpc() {}

  public static final String SERVICE_NAME = "com.ibm.watson.tas.proto.TasRuntime";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getAddModelMethod()} instead. 
  public static final io.grpc.MethodDescriptor<com.ibm.watson.tas.proto.TasRuntimeOuterClass.AddModelRequest,
      com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> METHOD_ADD_MODEL = getAddModelMethod();

  private static volatile io.grpc.MethodDescriptor<com.ibm.watson.tas.proto.TasRuntimeOuterClass.AddModelRequest,
      com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> getAddModelMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<com.ibm.watson.tas.proto.TasRuntimeOuterClass.AddModelRequest,
      com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> getAddModelMethod() {
    io.grpc.MethodDescriptor<com.ibm.watson.tas.proto.TasRuntimeOuterClass.AddModelRequest, com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> getAddModelMethod;
    if ((getAddModelMethod = TasRuntimeGrpc.getAddModelMethod) == null) {
      synchronized (TasRuntimeGrpc.class) {
        if ((getAddModelMethod = TasRuntimeGrpc.getAddModelMethod) == null) {
          TasRuntimeGrpc.getAddModelMethod = getAddModelMethod = 
              io.grpc.MethodDescriptor.<com.ibm.watson.tas.proto.TasRuntimeOuterClass.AddModelRequest, com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "com.ibm.watson.tas.proto.TasRuntime", "addModel"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ibm.watson.tas.proto.TasRuntimeOuterClass.AddModelRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo.getDefaultInstance()))
                  .setSchemaDescriptor(new TasRuntimeMethodDescriptorSupplier("addModel"))
                  .build();
          }
        }
     }
     return getAddModelMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getDeleteModelMethod()} instead. 
  public static final io.grpc.MethodDescriptor<com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelRequest,
      com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelResponse> METHOD_DELETE_MODEL = getDeleteModelMethod();

  private static volatile io.grpc.MethodDescriptor<com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelRequest,
      com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelResponse> getDeleteModelMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelRequest,
      com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelResponse> getDeleteModelMethod() {
    io.grpc.MethodDescriptor<com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelRequest, com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelResponse> getDeleteModelMethod;
    if ((getDeleteModelMethod = TasRuntimeGrpc.getDeleteModelMethod) == null) {
      synchronized (TasRuntimeGrpc.class) {
        if ((getDeleteModelMethod = TasRuntimeGrpc.getDeleteModelMethod) == null) {
          TasRuntimeGrpc.getDeleteModelMethod = getDeleteModelMethod = 
              io.grpc.MethodDescriptor.<com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelRequest, com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "com.ibm.watson.tas.proto.TasRuntime", "deleteModel"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new TasRuntimeMethodDescriptorSupplier("deleteModel"))
                  .build();
          }
        }
     }
     return getDeleteModelMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetModelStatusMethod()} instead. 
  public static final io.grpc.MethodDescriptor<com.ibm.watson.tas.proto.TasRuntimeOuterClass.GetStatusRequest,
      com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> METHOD_GET_MODEL_STATUS = getGetModelStatusMethod();

  private static volatile io.grpc.MethodDescriptor<com.ibm.watson.tas.proto.TasRuntimeOuterClass.GetStatusRequest,
      com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> getGetModelStatusMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<com.ibm.watson.tas.proto.TasRuntimeOuterClass.GetStatusRequest,
      com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> getGetModelStatusMethod() {
    io.grpc.MethodDescriptor<com.ibm.watson.tas.proto.TasRuntimeOuterClass.GetStatusRequest, com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> getGetModelStatusMethod;
    if ((getGetModelStatusMethod = TasRuntimeGrpc.getGetModelStatusMethod) == null) {
      synchronized (TasRuntimeGrpc.class) {
        if ((getGetModelStatusMethod = TasRuntimeGrpc.getGetModelStatusMethod) == null) {
          TasRuntimeGrpc.getGetModelStatusMethod = getGetModelStatusMethod = 
              io.grpc.MethodDescriptor.<com.ibm.watson.tas.proto.TasRuntimeOuterClass.GetStatusRequest, com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "com.ibm.watson.tas.proto.TasRuntime", "getModelStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ibm.watson.tas.proto.TasRuntimeOuterClass.GetStatusRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo.getDefaultInstance()))
                  .setSchemaDescriptor(new TasRuntimeMethodDescriptorSupplier("getModelStatus"))
                  .build();
          }
        }
     }
     return getGetModelStatusMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getEnsureLoadedMethod()} instead. 
  public static final io.grpc.MethodDescriptor<com.ibm.watson.tas.proto.TasRuntimeOuterClass.EnsureLoadedRequest,
      com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> METHOD_ENSURE_LOADED = getEnsureLoadedMethod();

  private static volatile io.grpc.MethodDescriptor<com.ibm.watson.tas.proto.TasRuntimeOuterClass.EnsureLoadedRequest,
      com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> getEnsureLoadedMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<com.ibm.watson.tas.proto.TasRuntimeOuterClass.EnsureLoadedRequest,
      com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> getEnsureLoadedMethod() {
    io.grpc.MethodDescriptor<com.ibm.watson.tas.proto.TasRuntimeOuterClass.EnsureLoadedRequest, com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> getEnsureLoadedMethod;
    if ((getEnsureLoadedMethod = TasRuntimeGrpc.getEnsureLoadedMethod) == null) {
      synchronized (TasRuntimeGrpc.class) {
        if ((getEnsureLoadedMethod = TasRuntimeGrpc.getEnsureLoadedMethod) == null) {
          TasRuntimeGrpc.getEnsureLoadedMethod = getEnsureLoadedMethod = 
              io.grpc.MethodDescriptor.<com.ibm.watson.tas.proto.TasRuntimeOuterClass.EnsureLoadedRequest, com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "com.ibm.watson.tas.proto.TasRuntime", "ensureLoaded"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ibm.watson.tas.proto.TasRuntimeOuterClass.EnsureLoadedRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo.getDefaultInstance()))
                  .setSchemaDescriptor(new TasRuntimeMethodDescriptorSupplier("ensureLoaded"))
                  .build();
          }
        }
     }
     return getEnsureLoadedMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static TasRuntimeStub newStub(io.grpc.Channel channel) {
    return new TasRuntimeStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static TasRuntimeBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new TasRuntimeBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static TasRuntimeFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new TasRuntimeFutureStub(channel);
  }

  /**
   * <pre>
   * this is a grpc version of the external tas-runtime interface
   * for managing and serving models
   * </pre>
   */
  public static abstract class TasRuntimeImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Adds/registers a trained model to this TAS runtime cluster
     * </pre>
     */
    public void addModel(com.ibm.watson.tas.proto.TasRuntimeOuterClass.AddModelRequest request,
        io.grpc.stub.StreamObserver<com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> responseObserver) {
      asyncUnimplementedUnaryCall(getAddModelMethod(), responseObserver);
    }

    /**
     * <pre>
     * Deletes/unregisters a model from this TAS runtime cluster,
     * has no effect if the specified model isn't found
     * </pre>
     */
    public void deleteModel(com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelRequest request,
        io.grpc.stub.StreamObserver<com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getDeleteModelMethod(), responseObserver);
    }

    /**
     * <pre>
     * Returns the status of the specified model. See the ModelStatus enum
     * </pre>
     */
    public void getModelStatus(com.ibm.watson.tas.proto.TasRuntimeOuterClass.GetStatusRequest request,
        io.grpc.stub.StreamObserver<com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> responseObserver) {
      asyncUnimplementedUnaryCall(getGetModelStatusMethod(), responseObserver);
    }

    /**
     * <pre>
     * Ensures the model with the specified id is loaded in this TAS cluster
     * </pre>
     */
    public void ensureLoaded(com.ibm.watson.tas.proto.TasRuntimeOuterClass.EnsureLoadedRequest request,
        io.grpc.stub.StreamObserver<com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> responseObserver) {
      asyncUnimplementedUnaryCall(getEnsureLoadedMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getAddModelMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ibm.watson.tas.proto.TasRuntimeOuterClass.AddModelRequest,
                com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo>(
                  this, METHODID_ADD_MODEL)))
          .addMethod(
            getDeleteModelMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelRequest,
                com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelResponse>(
                  this, METHODID_DELETE_MODEL)))
          .addMethod(
            getGetModelStatusMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ibm.watson.tas.proto.TasRuntimeOuterClass.GetStatusRequest,
                com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo>(
                  this, METHODID_GET_MODEL_STATUS)))
          .addMethod(
            getEnsureLoadedMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ibm.watson.tas.proto.TasRuntimeOuterClass.EnsureLoadedRequest,
                com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo>(
                  this, METHODID_ENSURE_LOADED)))
          .build();
    }
  }

  /**
   * <pre>
   * this is a grpc version of the external tas-runtime interface
   * for managing and serving models
   * </pre>
   */
  public static final class TasRuntimeStub extends io.grpc.stub.AbstractStub<TasRuntimeStub> {
    private TasRuntimeStub(io.grpc.Channel channel) {
      super(channel);
    }

    private TasRuntimeStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TasRuntimeStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new TasRuntimeStub(channel, callOptions);
    }

    /**
     * <pre>
     * Adds/registers a trained model to this TAS runtime cluster
     * </pre>
     */
    public void addModel(com.ibm.watson.tas.proto.TasRuntimeOuterClass.AddModelRequest request,
        io.grpc.stub.StreamObserver<com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getAddModelMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Deletes/unregisters a model from this TAS runtime cluster,
     * has no effect if the specified model isn't found
     * </pre>
     */
    public void deleteModel(com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelRequest request,
        io.grpc.stub.StreamObserver<com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getDeleteModelMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Returns the status of the specified model. See the ModelStatus enum
     * </pre>
     */
    public void getModelStatus(com.ibm.watson.tas.proto.TasRuntimeOuterClass.GetStatusRequest request,
        io.grpc.stub.StreamObserver<com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetModelStatusMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Ensures the model with the specified id is loaded in this TAS cluster
     * </pre>
     */
    public void ensureLoaded(com.ibm.watson.tas.proto.TasRuntimeOuterClass.EnsureLoadedRequest request,
        io.grpc.stub.StreamObserver<com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getEnsureLoadedMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * this is a grpc version of the external tas-runtime interface
   * for managing and serving models
   * </pre>
   */
  public static final class TasRuntimeBlockingStub extends io.grpc.stub.AbstractStub<TasRuntimeBlockingStub> {
    private TasRuntimeBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private TasRuntimeBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TasRuntimeBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new TasRuntimeBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Adds/registers a trained model to this TAS runtime cluster
     * </pre>
     */
    public com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo addModel(com.ibm.watson.tas.proto.TasRuntimeOuterClass.AddModelRequest request) {
      return blockingUnaryCall(
          getChannel(), getAddModelMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Deletes/unregisters a model from this TAS runtime cluster,
     * has no effect if the specified model isn't found
     * </pre>
     */
    public com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelResponse deleteModel(com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelRequest request) {
      return blockingUnaryCall(
          getChannel(), getDeleteModelMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Returns the status of the specified model. See the ModelStatus enum
     * </pre>
     */
    public com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo getModelStatus(com.ibm.watson.tas.proto.TasRuntimeOuterClass.GetStatusRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetModelStatusMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Ensures the model with the specified id is loaded in this TAS cluster
     * </pre>
     */
    public com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo ensureLoaded(com.ibm.watson.tas.proto.TasRuntimeOuterClass.EnsureLoadedRequest request) {
      return blockingUnaryCall(
          getChannel(), getEnsureLoadedMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * this is a grpc version of the external tas-runtime interface
   * for managing and serving models
   * </pre>
   */
  public static final class TasRuntimeFutureStub extends io.grpc.stub.AbstractStub<TasRuntimeFutureStub> {
    private TasRuntimeFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private TasRuntimeFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TasRuntimeFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new TasRuntimeFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Adds/registers a trained model to this TAS runtime cluster
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> addModel(
        com.ibm.watson.tas.proto.TasRuntimeOuterClass.AddModelRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getAddModelMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Deletes/unregisters a model from this TAS runtime cluster,
     * has no effect if the specified model isn't found
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelResponse> deleteModel(
        com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getDeleteModelMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Returns the status of the specified model. See the ModelStatus enum
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> getModelStatus(
        com.ibm.watson.tas.proto.TasRuntimeOuterClass.GetStatusRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetModelStatusMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Ensures the model with the specified id is loaded in this TAS cluster
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo> ensureLoaded(
        com.ibm.watson.tas.proto.TasRuntimeOuterClass.EnsureLoadedRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getEnsureLoadedMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_ADD_MODEL = 0;
  private static final int METHODID_DELETE_MODEL = 1;
  private static final int METHODID_GET_MODEL_STATUS = 2;
  private static final int METHODID_ENSURE_LOADED = 3;
  private static final int METHODID_APPLY_MODEL = 4;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final TasRuntimeImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(TasRuntimeImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_ADD_MODEL:
          serviceImpl.addModel((com.ibm.watson.tas.proto.TasRuntimeOuterClass.AddModelRequest) request,
              (io.grpc.stub.StreamObserver<com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo>) responseObserver);
          break;
        case METHODID_DELETE_MODEL:
          serviceImpl.deleteModel((com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelRequest) request,
              (io.grpc.stub.StreamObserver<com.ibm.watson.tas.proto.TasRuntimeOuterClass.DeleteModelResponse>) responseObserver);
          break;
        case METHODID_GET_MODEL_STATUS:
          serviceImpl.getModelStatus((com.ibm.watson.tas.proto.TasRuntimeOuterClass.GetStatusRequest) request,
              (io.grpc.stub.StreamObserver<com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo>) responseObserver);
          break;
        case METHODID_ENSURE_LOADED:
          serviceImpl.ensureLoaded((com.ibm.watson.tas.proto.TasRuntimeOuterClass.EnsureLoadedRequest) request,
              (io.grpc.stub.StreamObserver<com.ibm.watson.tas.proto.TasRuntimeOuterClass.ModelStatusInfo>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class TasRuntimeBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    TasRuntimeBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ibm.watson.tas.proto.TasRuntimeOuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("TasRuntime");
    }
  }

  private static final class TasRuntimeFileDescriptorSupplier
      extends TasRuntimeBaseDescriptorSupplier {
    TasRuntimeFileDescriptorSupplier() {}
  }

  private static final class TasRuntimeMethodDescriptorSupplier
      extends TasRuntimeBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    TasRuntimeMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (TasRuntimeGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new TasRuntimeFileDescriptorSupplier())
              .addMethod(getAddModelMethod())
              .addMethod(getDeleteModelMethod())
              .addMethod(getGetModelStatusMethod())
              .addMethod(getEnsureLoadedMethod())
              .build();
        }
      }
    }
    return result;
  }
}
