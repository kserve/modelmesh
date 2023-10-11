[![Build](https://github.com/kserve/modelmesh/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/kserve/modelmesh/actions/workflows/build.yml)

# ModelMesh

The ModelMesh framework is a mature, general-purpose model serving management/routing layer designed for high-scale, high-density and frequently-changing model use cases. It works with existing or custom-built model servers and acts as a distributed LRU cache for serving runtime models.

For full Kubernetes-based deployment and management of ModelMesh clusters and models, see the [ModelMesh Serving](https://github.com/kserve/modelmesh-serving) repo. This includes a separate controller and provides K8s custom resource based management of ServingRuntimes and InferenceServices along with common, abstracted handling of model repository storage and ready-to-use integrations with some existing OSS model servers.

For more information on supported features and design details, see [these charts](https://github.com/kserve/modelmesh/files/8854091/modelmesh-jun2022.pdf).

## Get Started

To get started with the ModelMesh framework, check out [this guide](/docs/overview.md).

## Developer guide

Check out the [developer guide](developer-guide.md) to learn about development practices for the project.
