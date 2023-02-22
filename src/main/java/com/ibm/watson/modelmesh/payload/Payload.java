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

import java.lang.reflect.Method;
import java.util.Arrays;

import io.grpc.Metadata;
import io.netty.buffer.ByteBuf;

public class Payload {

    private final String modelId;

    private final String vModelId;

    private final String method;

    private final Metadata metadata;

    private final ByteBuf data;

    public Payload(String modelId, String vModelId, String method, Metadata metadata, ByteBuf data) {
        this.modelId = modelId;
        this.vModelId = vModelId;
        this.method = method;
        this.metadata = metadata;
        this.data = data;
    }

    public String getModelId() {
        return modelId;
    }

    public String getVModelId() {
        return vModelId;
    }

    public String getMethod() {
        return method;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public ByteBuf getData() {
        return data;
    }

    @Override
    public String toString() {
        return "Payload{" +
                "modelId='" + modelId + '\'' +
                ", vModelId='" + vModelId + '\'' +
                ", method='" + method + '\'' +
                ", metadata=" + metadata +
                ", data=" + data +
                '}';
    }
}
