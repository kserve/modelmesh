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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.Metadata;
import io.grpc.Status;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

/**
 * A model-mesh payload.
 */
public class Payload {

    public enum Kind {
        REQUEST,
        RESPONSE
    }

    private final String id;

    private final String modelId;

    private final String vModelId;

    private final String method;

    private final Metadata metadata;

    private final ByteBuf data;

    // null for requests, non-null for responses
    private final Status status;


    public Payload(@Nonnull String id, @Nonnull String modelId, @Nullable String method, @Nullable Metadata metadata,
                   @Nullable ByteBuf data, @Nullable Status status) {
        this(id, modelId, null, method, metadata, data, status);
    }

    public Payload(@Nonnull String id, @Nonnull String modelId, @Nullable String vModelId, @Nullable String method,
                   @Nullable Metadata metadata, @Nullable ByteBuf data, @Nullable Status status) {
        this.id = id;
        this.modelId = modelId;
        this.vModelId = vModelId;
        this.method = method;
        this.metadata = metadata;
        this.data = data;
        this.status = status;
    }

    @Nonnull
    public String getId() {
        return id;
    }

    @Nonnull
    public String getModelId() {
        return modelId;
    }

    @CheckForNull
    public String getVModelId() {
        return vModelId;
    }

    @Nonnull
    public String getVModelIdOrModelId() {
        return vModelId != null ? vModelId : modelId;
    }

    @CheckForNull
    public String getMethod() {
        return method;
    }

    @CheckForNull
    public Metadata getMetadata() {
        return metadata;
    }

    @CheckForNull
    public ByteBuf getData() {
        return data;
    }

    @Nonnull
    public Kind getKind() {
        return status == null ? Kind.REQUEST : Kind.RESPONSE;
    }

    @Nullable
    public Status getStatus() {
        return status;
    }

    public void release() {
        ReferenceCountUtil.release(this.data);
    }

    @Override
    public String toString() {
        return "Payload{" +
                "id='" + id + '\'' +
                ", vModelId=" + (vModelId != null ? ('\'' + vModelId + '\'') : "null") +
                ", modelId='" + modelId + '\'' +
                ", method='" + method + '\'' +
                ", status=" + (status == null ? "request" : String.valueOf(status)) +
                '}';
    }
}
