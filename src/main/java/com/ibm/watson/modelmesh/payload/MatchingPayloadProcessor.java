/*
 * Copyright 2023 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.ibm.watson.modelmesh.payload;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link PayloadProcessor} that processes {@link Payload}s only if they match with given model ID or method name.
 */
public class MatchingPayloadProcessor implements PayloadProcessor {

    private final PayloadProcessor delegate;

    private final String methodName;

    private final String modelId;

    private final String vModelId;

    MatchingPayloadProcessor(PayloadProcessor delegate, String methodName, String modelId, String vModelId) {
        this.delegate = delegate;
        this.methodName = methodName;
        this.modelId = modelId;
        this.vModelId = vModelId;
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public boolean process(Payload payload) {
        boolean methodMatches = this.methodName == null || Objects.equals(this.methodName, payload.getMethod());
        if (methodMatches) {
            boolean modelIdMatches = this.modelId == null || this.modelId.equals(payload.getModelId());
            if (modelIdMatches) {
                boolean vModelIdMatches = this.vModelId == null || this.vModelId.equals(payload.getVModelId());
                if (vModelIdMatches) {
                    return delegate.process(payload);
                }
            }
        }
        return false;
    }

    public static MatchingPayloadProcessor from(String modelId, String method, PayloadProcessor processor) {
        return from(modelId, null, method, processor);
    }

    public static MatchingPayloadProcessor from(String modelId, String vModelId,
                                                String method, PayloadProcessor processor) {
        if (modelId != null) {
            if (!modelId.isEmpty()) {
                modelId = modelId.replaceFirst("/", "");
                if (modelId.isEmpty() || modelId.equals("*")) {
                    modelId = null;
                }
            } else {
                modelId = null;
            }
        }
        if (vModelId != null) {
            if (!vModelId.isEmpty()) {
                vModelId = vModelId.replaceFirst("/", "");
                if (vModelId.isEmpty() || vModelId.equals("*")) {
                    vModelId = null;
                }
            } else {
                vModelId = null;
            }
        }
        if (method != null && (method.isEmpty() || method.equals("*"))) {
            method = null;
        }
        return new MatchingPayloadProcessor(processor, method, modelId, vModelId);
    }

    @Override
    public void close() throws IOException {
        this.delegate.close();
    }
}
