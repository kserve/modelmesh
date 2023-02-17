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

package com.ibm.watson.modelmesh.processor;

import java.lang.reflect.Method;
import java.util.Arrays;

public class Payload {

    private final String modelId;

    private final Method method;

    private final Method remoteMethod;

    private final Object[] gRPCArgs;

    private final Object modelResponse;

    public Payload(String modelId, Method method, Method remoteMethod, Object[] gRPCArgs, Object modelResponse) {
        this.modelId = modelId;
        this.method = method;
        this.remoteMethod = remoteMethod;
        this.gRPCArgs = gRPCArgs;
        this.modelResponse = modelResponse;
    }

    public String getModelId() {
        return modelId;
    }

    public Method getMethod() {
        return method;
    }

    public Method getRemoteMethod() {
        return remoteMethod;
    }

    public Object[] getGRPCArgs() {
        return gRPCArgs;
    }

    public Object getModelResponse() {
        return modelResponse;
    }

    @Override
    public String toString() {
        return "Payload{" +
                "modelId='" + modelId + '\'' +
                ", method=" + method +
                ", remoteMethod=" + remoteMethod +
                ", gRPCArgs=" + Arrays.toString(gRPCArgs) +
                ", modelResponse=" + modelResponse +
                '}';
    }
}
