Processing model-mesh payloads
=============================

`Model-mesh` exchange `Payloads` with the models deployed within runtimes.
In `model-mesh` a `Payload` consists of information regarding the id of the model and the _method_ of the model being called, together with some data (actual binary requests or responses) and metadata (e.g., headers).
A `PayloadProcessor` is responsible for processing such `Payloads` for models served by `model-mesh`.

Reasonable examples of `PayloadProcessors` include loggers of prediction requests, data sinks for data visualization, model quality assessment or monitoring tooling.

A `PayloadProcessor` can be configured to only look at payloads that are consumed and produced by certain models, or payloads containing certain headers, etc.
This configuration is performed at `ModelMesh` instance level.
Multiple `PayloadProcessors` can be configured per each `ModelMesh` instance.

Implementations of `PayloadProcessors` can care about only specific portions of the payload (e.g., model inputs, model outputs, metadata, specific headers, etc.).

A `PayloadProcessor` can see input data like the one in this example:
```text
[mmesh.ExamplePredictor/predict, Metadata(content-type=application/grpc,user-agent=grpc-java-netty/1.51.1,mm-model-id=myModel,another-custom-header=custom-value,grpc-accept-encoding=gzip,grpc-timeout=1999774u), CompositeByteBuf(ridx: 0, widx: 2000004, cap: 2000004, components=147)
```

A `PayloadProcessor` can see output data as `ByteBuf` like the one in this example:
```text
java.nio.HeapByteBuffer[pos=0 lim=65 cap=65]
```

A `PayloadProcessor` can be configured by means of a whitespace separated `String` of URIs.
In a URI like `logger:///*?pytorch1234#predict`: 
* the scheme represents the type of processor, e.g., `logger`
* the query represents the model id to observe, e.g., `pytorch1234`
* the fragment represents the method to observe, e.g., `predict`

Featured `PayloadProcessors`:
* `logger` : logs requests/responses payloads  to `model-mesh` logs (_INFO_ level), e.g., use `logger://*` to log every `Payload`
* `http` : sends requests/responses payloads to a remote service (via _HTTP POST_), e.g., use `http://10.10.10.1:8080/consumer/kserve/v2` to send every `Payload` to the specified HTTP endpoint