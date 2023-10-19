# Overview

ModelMesh is a mature, general-purpose model serving management/routing layer designed for high-scale, high-density and frequently-changing model use cases. It works with existing or custom-built model servers and acts as a distributed LRU cache for serving runtime models.

For full Kubernetes-based deployment and management of ModelMesh clusters and models, see the [ModelMesh Serving](https://github.com/kserve/modelmesh-serving) repo. This includes a separate controller and provides K8s custom resource based management of ServingRuntimes and InferenceServices along with common, abstracted handling of model repository storage and ready-to-use integrations with some existing OSS model servers.

For more information on supported features and design details, see [these charts](https://github.com/kserve/modelmesh/files/8854091/modelmesh-jun2022.pdf).

## What is a model?

In ModelMesh, a **model** refers to an abstraction of machine learning models. It is not aware of the underlying model format. There are two model types: model (regular) and vmodel. Regular models in ModelMesh are assumed and required to be immutable. VModels add a layer of indirection in front of the immutable models. See [VModels Reference](/docs/vmodels.md) for further reading.

## Implement a model runtime

1. Wrap your model-loading and invocation logic in this [model-runtime.proto](/src/main/proto/current/model-runtime.proto) gRPC service interface.
   - `runtimeStatus()` - called only during startup to obtain some basic configuration parameters from the runtime, such as version, capacity, model-loading timeout.
   - `loadModel()` - load the specified model into memory from backing storage, returning when complete.
   - `modelSize()` - determine size (memory usage) of previously-loaded model. If very fast, can be omitted and provided instead in the response from `loadModel`.
   - `unloadModel()` - unload previously loaded model, returning when complete.
   - Use a separate, arbitrary gRPC service interface for model inferencing requests. It can have any number of methods and they are assumed to be idempotent. See [predictor.proto](/src/test/proto/predictor.proto) for a very simple example.
   - The methods of your custom applier interface will be called only for already fully-loaded models.
2. Build a grpc server docker container which exposes these interfaces on localhost port 8085 or via a mounted unix domain socket.
3. Extend the [Kustomize-based Kubernetes manifests](/config) to use your docker image, and with appropriate memory and CPU resource allocations for your container.
4. Deploy to a Kubernetes cluster as a regular Service, which will expose [this grpc service interface](/src/main/proto/current/model-mesh.proto) via kube-dns (you do not implement this yourself), consume using grpc client of your choice from your upstream service components.
   - `registerModel()` and `unregisterModel()` for registering/removing models managed by the cluster
   - Any custom inferencing interface methods to make a runtime invocation of previously-registered model, making sure to set a `mm-model-id` or `mm-vmodel-id` metadata header (or `-bin` suffix equivalents for UTF-8 ids)

## Development

Please see the [Developer Guide](/developer-guide.md) for details.