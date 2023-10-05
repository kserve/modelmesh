# ModelMesh

The ModelMesh framework is a mature, general-purpose model serving management/routing layer designed for high-scale, high-density and frequently-changing model use cases. It works with existing or custom-built model servers and acts as a distributed LRU cache for serving runtime models.

See these [charts](https://github.com/kserve/modelmesh/files/8854091/modelmesh-jun2022.pdf) for more information on supported features and design details.

For full Kubernetes-based deployment and management of ModelMesh clusters and models, see the [ModelMesh Serving](https://github.com/kserve/modelmesh-serving) repo. This includes a separate controller and provides K8s custom resource based management of ServingRuntimes and InferenceServices along with common, abstracted handling of model repository storage and ready-to-use integrations with some existing OSS model servers.

## Get Started

To get started with the ModelMesh framework, check out [this guide](/docs/overview.md).
