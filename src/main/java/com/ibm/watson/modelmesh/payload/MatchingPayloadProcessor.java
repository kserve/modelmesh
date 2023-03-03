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

public class MatchingPayloadProcessor implements PayloadProcessor {

    private final PayloadProcessor delegate;

    private final String methodName;

    private final String modelId;

    MatchingPayloadProcessor(PayloadProcessor delegate, String methodName, String modelId) {
        this.delegate = delegate;
        this.methodName = methodName;
        this.modelId = modelId;
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public void process(Payload payload) {
        boolean methodMatches = true;
        if (this.methodName != null) {
            methodMatches = payload.getMethod() != null && this.methodName.equals(payload.getMethod());
        }
        if (methodMatches) {
            boolean modelIdMatches = true;
            if (this.modelId != null) {
                modelIdMatches = this.modelId.equals(payload.getModelId()) || this.modelId.equals(payload.getVModelId());
            }
            if (modelIdMatches) {
                delegate.process(payload);
            }
        }
    }

    public static MatchingPayloadProcessor from(String modelId, String method, PayloadProcessor processor) {
        if (modelId != null) {
            if (modelId.length() > 0) {
                modelId = modelId.replaceFirst("/", "");
                if (modelId.length() == 0 || modelId.equals("*")) {
                    modelId = null;
                }
            } else {
                modelId = null;
            }
        }
        if (method != null) {
            if (method.length() == 0 || method.equals("*")) {
                method = null;
            }
        }
        return new MatchingPayloadProcessor(processor, method, modelId);
    }
}
