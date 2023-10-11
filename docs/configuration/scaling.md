ModelMesh relies on [Kubernetes for rolling updates](https://kubernetes.io/docs/tutorials/kubernetes-basics/update/update-intro/). For the sake of simplicity and elasticity, ModelMesh does not keep track of update states or so internally.

## Scaling Up/Down

ModelMesh follows the process below, skipping the termination/migration steps in the context of scaling up (adding new pods).

1. A new Pod with updates starts. 
2. Kubernetes awaits the new Pod to report `Ready` state.
3. If ready, it triggers termination of the old Pod. 
4. Once the old Pod receives a termination signal from Kubernetes, it will begin to migrate its models to other instances. 

Asynchronously, ModelMesh will try to rebalance model distribution among all the pods with `Ready` state.

## Fail Fast with Readiness Probes

When an update triggers a cluster-wise failure, resulting in the failure to load existing models on new pods, fail fast protection will prevent old cluster from shutting down completely by using [Readiness Probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-readiness-probes).

ModelMesh achieves fail fast by collecting statistics about loading failures during the startup period. Specifically:

1. Critical Failure - if this model loaded successfully on other pods, but cannot be loaded on this pod.
2. General Failure - if a new model cannot be loaded on this pod.

However, this statistics are only collected during the startup period. The length of this period can be controlled by the environment variable `BOOTSTRAP_CLEARANCE_PERIOD_MS`. Once failure statistics exceed the threshold on certain pods, these pods will start to report a `NOT READY` state. This will prevent the old pods from terminating.

The default `BOOTSTRAP_CLEARANCE_PERIOD_MS` is 3 minutes (180,000 ms).

**Note**: you may also want to tweak the readiness probes' parameters as well. For example, increasing `initialDelaySeconds` may help slow down the shutdown old pods too early.

## Rolling Update Configuration

Specify `maxUnavailable` and `maxSurge` [as described here](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#rolling-update-deployment) to control the rolling update process.