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

import java.net.URI;

import io.grpc.Metadata;
import io.grpc.Status;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

class RemotePayloadProcessorTest {

    @Test
    void testDestinationUnreachable() {
        RemotePayloadProcessor remotePayloadProcessor = new RemotePayloadProcessor(URI.create("http://this-does-not-exist:123"));
        String id = "123";
        String modelId = "456";
        String method = "predict";
        Status kind = Status.INVALID_ARGUMENT;
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");
        metadata.put(Metadata.Key.of("binary-bin", Metadata.BINARY_BYTE_MARSHALLER), "string".getBytes());
        ByteBuf data = Unpooled.buffer(4);
        Payload payload = new Payload(id, modelId, method, metadata, data, kind);
        assertFalse(remotePayloadProcessor.process(payload));
    }
}