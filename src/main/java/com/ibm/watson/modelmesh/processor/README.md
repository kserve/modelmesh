Processing model-mesh payloads
=============================

In `model-mesh` the id of a given model, the method being called with its inputs and associated outputs form the `Payload`.
A `PayloadProcessor` is responsible for processing such `Payloads` for models served by `model-mesh`.

Reasonable examples of `PayloadProcessors` include loggers of model predictions, data sinks for visualization, model quality assessment or monitoring purposes.

A `PayloadProcessor` can be configured to only look at payloads that are consumed and produced by certain models, or payloads containing certain headers, etc.
This configuration is performed at `ModelMesh` instance level.
Multiple `PayloadProcessors` can be configured per each `ModelMesh` instance.

Implementations of `PayloadProcessors` can care about only specific portions of the payload (e.g., model inputs, model outputs, metadata, specific headers, etc.).

A `PayloadProcessor` sees input data like the one in this example:
```text
[mmesh.ExamplePredictor/predict, Metadata(content-type=application/grpc,user-agent=grpc-java-netty/1.51.1,mm-model-id=myModel,another-custom-header=custom-value,grpc-accept-encoding=gzip,grpc-timeout=1999774u), CompositeByteBuf(ridx: 0, widx: 2000004, cap: 2000004, components=147)
```

A `PayloadProcessor` sees output data as `ByteBuf` like the one in this example:
```text
java.nio.HeapByteBuffer[pos=0 lim=65 cap=65]
```

A `PayloadProcessor` can be configured by means of a comma separated string of URIs.
In a URI like `logger://pytorch1234?predict`: 
* the scheme represents the type of processor, e.g., `logger`
* the authority represents the model id to observe, e.g., `pytorch1234` 
* the query represents the method to observe, e.g., `predict`
