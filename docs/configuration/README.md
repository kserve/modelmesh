A core goal of the ModelMesh framework was minimizing the amount of custom configuration required. It should be possible to get up and running without changing most of these things.

## Model Runtime Configuration

There are a few basic parameters (some optional) that the model runtime implementation must report in a `RuntimeStatusResponse` response to the `ModelRuntime.runtimeStatus` rpc method once it has successfully initialized:

- `uint64 capacityInBytes`
- `uint32 maxLoadingConcurrency`
- `uint32 modelLoadingTimeoutMs`
- `uint64 defaultModelSizeInBytes`
- `string runtimeVersion` (optional)
- ~~`uint64 numericRuntimeVersion`~~ (deprecated, unused)
- `map<string,MethodInfo> methodInfos` (optional)
- `bool allowAnyMethod` - applicable only if one or more `methodInfos` are provided.
- `bool limitModelConcurrency` - (experimental)

It's expected that all model runtime instances in the same cluster (with same Kubernetes deployment config including image version) will report the same values for these, although it's not strictly necessary.

## TLS (SSL) Configuration

This can be configured via environment variables on the ModelMesh container, refer to [the documentation](/docs/configuration/tls.md).

## Model Auto-Scaling

Nothing needs to be configured to enable this, it is on by default. There is a single configuration parameter which can optionally be used to tune the sensitivity of the scaling, based on rate of requests per model. Note that this applies to scaling copies of models within existing pods, not scaling of the pods themselves.

The scale-up RPM threshold specifies a target request rate per model **copy** measured in requests per minute. Model-mesh balances requests between loaded copies of a given model evenly, and if one copy's share of requests increases above this threshold more copies will be added if possible in instances (replicas) that do not currently have the model loaded.

The default for this parameter is 2000 RPM. It can be overridden by setting either the `MM_SCALEUP_RPM_THRESHOLD` environment variable or `scaleup_rpm_threshold` etcd/zookeeper dynamic config parameter, with the latter taking precedence.

Other points to note:

- Scale up can happen by more than one additional copy at a time if the request rate breaches the configured threshold by a sufficient amount.
- The number of replicas in the deployment dictates the maximum number of copies that a given model can be scaled to (one in each Pod).
- Models will scale to two copies if they have been used recently regardless of the load - the autoscaling behaviour applies between 2 and N>2 copies.
- Scale-down will occur slowly once the per-copy load remains below the configured threshold for long enough.
- Note that if the runtime is in latency-based auto-scaling mode (when the runtime returns non-default `limitModelConcurrency = true` in the `RuntimeStatusResponse`), scaling is triggered based on measured latencies/queuing rather than request rates, and the RPM threshold parameter will have no effect. 

## Request Header Logging

To have particular gRPC request metadata headers included in any request-scoped log messages, set the `MM_LOG_REQUEST_HEADERS` environment variable to a json string->string map (object) whose keys are the header names to log and values are the names of corresponding entries to insert into the logger thread context map (MDC).

Values can be either raw ascii or base64-encoded utf8; in the latter case the corresponding header name must end with `-bin`. For example:
```
{
    "transaction_id": "txid",
    "user_id-bin": "user_id"
}
```
**Note**: this does not generate new log messages and successful requests aren't logged by default. To log a message for every request, additionally set the `MM_LOG_EACH_INVOKE` environment variable to true.

## Other Optional Parameters

Set via environment variables on the ModelMesh container:

- `MM_SVC_GRPC_PORT` - external grpc port, default 8033
- `INTERNAL_GRPC_SOCKET_PATH` - unix domain socket, which should be a file location on a persistent volume mounted in both the model-mesh and model runtime containers, defaults to /tmp/mmesh/grpc.sock
- `INTERNAL_SERVING_GRPC_SOCKET_PATH` - unix domain socket to use for inferencing requests, defaults to be same as primary domain socket
- `INTERNAL_GRPC_PORT` - pod-internal grpc port (model runtime localhost), default 8056
- `INTERNAL_SERVING_GRPC_PORT` - pod-internal grpc port to use for inferencing requests, defaults to be same as primary pod-internal grpc port
- `MM_SVC_GRPC_MAX_MSG_SIZE` - max message size in bytes, default 16MiB
- `MM_SVC_GRPC_MAX_HEADERS_SIZE` - max headers size in bytes, defaults to gRPC default
- `MM_METRICS` - metrics configuration, see Metrics wiki page
- `MM_MULTI_PARALLELISM` - max multi-model request parallelism, default 4
- `KV_READ_ONLY` (advanced) - run in "read only" mode where new (v)models cannot be registered or unregistered
- `MM_LOG_EACH_INVOKE` - log an INFO level message for every request; default is false, set to true to enable
- `MM_SCALEUP_RPM_THRESHOLD` - see Model auto-scaling above

**Note**: only one of `INTERNAL_GRPC_SOCKET_PATH` and `INTERNAL_GRPC_PORT` can be set. The same goes for `INTERNAL_SERVING_GRPC_SOCKET_PATH` and `INTERNAL_SERVING_GRPC_PORT`.

Set dynamically in kv-store (etcd or zookeeper):
- log_each_invocation - dynamic override of `MM_LOG_EACH_INVOKE` env var
- logger_level - TODO
- scaleup_rpm_threshold - dynamic override of `MM_SCALEUP_RPM_THRESHOLD` env var, see [auto-scaling](#model-auto-scaling) above.
