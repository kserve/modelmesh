## Deployment Configuration

This directory contains [Kustomize](https://kustomize.io/)-based layered configuration for deploying model-mesh in Kubernetes.

- `base` contains the core deployments upon which other configs sit ("overlays" in Kustomize parlance), along with a handful of patch files that can be selectively included in overlays to control various aspects of the deployed configuration

The `examples` directory contains example Kustomization overlays to demonstrate how to customize the above base configs for deployment in your own environment.

- `custom-example` is an example of an overlay to deploy model-mesh with a custom model-serving runtime image
- `custom-example-uds` extends `custom-example` to use a unix domain socket for intra-pod communication
- `type-constraints-example` is an example of a heterogeneous model-mesh deployment comprising two kubernetes Deployments with a single Service. It employs type constraints to control assignments of models to pod subsets based on labels.

The following patches are provided in `base/patches` and can be selectively included/modified in your custom overlay:

- `etcd.yaml` - configure the etcd server to use (required by model-mesh)
- `tls.yaml` - enable TLS (recommended) via a keypair secret
- `uds.yaml` - use a [Unix Domain Socket](https://github.com/kserve/modelmesh/wiki/Unix-domain-socket-communication) for intra-pod communication (high performance)
- `max_msg_size.yaml` - specify a custom max size for incoming gRPC message data and headers
- `prometheus_metrics.yaml` - turn on [prometheus-based publishing of metrics](https://github.com/kserve/modelmesh/wiki/Metrics)

### Network Policy

The `base` configuration includes a basic Kubernetes [NetworkPolicy](base/networkpolicy.yaml) that specifies standard ingress restrictions based on typical model-mesh inter-pod communication flows. This can be customized for your application as needed (for example changing the main service port from 8033 if you've overridden that modelmesh default), or excluded completely by removing its reference from the base [kustomization.yaml](base/kustomization.yaml).

### Making a deployment

Install the latest version of `kustomize` as described [here](https://kubectl.docs.kubernetes.io/installation/kustomize/), as well as an appropriate version of `kubectl`. Run for example:
```bash
$ kustomize --load-restrictor=none build examples/custom-example | kubectl apply -f -
service/model-mesh-example created
deployment.apps/model-mesh-example created

```

Note:
- `--load-restrictor=none` is required to allow overlays to access patch files outside of their own directory subtree
- Although `kubectl` now has native support for `kustomize`, it contains a version that does not yet support features used here (in particular there is not a way to specify `--load-restrictor=none` as far as I know).
