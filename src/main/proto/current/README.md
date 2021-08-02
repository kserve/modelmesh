# model-mesh-protos
Protocol-buffer service definitions for model-mesh gRPC APIs.

This repo can be included as a submodule in other consuming components, which should typically only use one or the other of the interfaces:

- [model-mesh.proto](model-mesh.proto) - Exposed by model-mesh, used by external clients to manage models/vmodels in a logical model-mesh cluster
- [model-runtime.proto](model-runtime.proto) - _Implemented_ by model runtime containers (deployed alongside model-mesh in the same pod), consumed by model-mesh
