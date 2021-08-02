### Heterogeneous cluster deployment

This directory contains example configuration for a heterogenous model cluster whose instances (pods) span more than one Kubernetes `Deployment` but otherwise behaves as a single logical service (still uses a single Kubernetes `Service`).

This can be used in cases where certain types have models have specific requirements/preferences tied to a specific model server implementation/configuration and/or to specific Node attributes.

Examples are:
- A Kubernetes cluster where only some nodes have GPUs and certain kinds of models may require or be able to exploit GPUs while other kinds of models in the same model-mesh service use CPU only
- Managing heterogenous model server runtimes in the same service - for example custom java-based and custom python-based. In this case the sub-deployments would form a disjoint partition of mostly-independently managed pods.
   - In particular this allows vmodels to transition between different underlying model runtime types, assuming that they expose the same inferencing gRPC API


#### Type-label mappings

Each pod (sub-deployment) can be assigned one or more labels. These are configured via the `MM_LABELS` environment variable on the `mm` container as a comma-separated list. Note these "labels" are separate to / independent of Kubernetes' native resource labels.

Constraints are defined per model type. Each model type may have zero or more _required_ labels and zero or more _preferred_ labels. Types for which no mapping is defined are assumed to have no particular requirements or preferences w.r.t. where their models are  _unless_ the special type name `_default` is included, in which case they will adopt its specified constraints. If _any_ required labels are specified for a particular type then models of that type will only ever be placed on pods which have _all_ of their required labels. Note that it does not make sense for a label to be included in both lists (requirement is strictly stronger than preference).

The type-constraint mapping is specified as a simple json doc which can be configured as either a literal value (`MM_TYPE_CONSTRAINTS` env var on `mm` container) or as a `ConfigMap` value (mounted to path in the `mm` container specified by the `MM_TYPE_CONSTRAINTS_PATH` env var). It's important that this configuration is always identical across all sub-deployments and thus using a shared `ConfigMap` is recommended. Changes to the type mappings in this `ConfigMap` will take effect dynamically without requiring any pod restarts.

Example type-constraint mapping json:

```json
{
  "type-name1": {
    "required": ["label1", "label2"]
  },
  "type-name2": {
    "preferred": ["label2"]
  },
  "type-name3": {
    "required": ["label1", "label3"],
    "preferred": ["label4"]
  },
  "_default": {
    "required": ["_unrecognized"]
  }
}
```

The **optional** `_default` type name can be used to enable behaviour where models of an unrecognized type will not be placed anywhere. Their status will indicate a loading failure with a message that there is no running instance that meets the type label requirement. Just specify a required label that will never be assigned to any of the sub-deployments.

#### Kustomize overlay arrangement

- Configuration that is common to all sub-deployments goes into the `runtime_patch_common.yaml` patch. This should always include the `MM_TYPE_CONSTRAINTS_PATH` or `MM_TYPE_CONSTRAINTS` env variable.
- Per-subdeployment configuration is applied as patches in the `gpu-deployment` and `non-gpu-deployment` kustomizations (rename and augment as appropriate). This should include the `MM_LABELS` environment variable.
