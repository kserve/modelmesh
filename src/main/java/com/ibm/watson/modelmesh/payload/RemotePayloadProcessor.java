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
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PayloadProcessor} that sends payloads to a remote service via HTTP POST.
 */
public class RemotePayloadProcessor extends PayloadDataProcessor {

    private final static Logger logger = LoggerFactory.getLogger(RemotePayloadProcessor.class);

    private final URI uri;
    private final HttpClient client;

    public RemotePayloadProcessor(URI uri) {
        this.uri = uri;
        this.client = HttpClient.newHttpClient();
    }

    @Override
    protected void processRequestPayload(Payload payload) {
        Map<String, Object> values = prepareContentBody(payload, "request");
        sendPayload(payload, values);
    }

    private static Map<String, Object> prepareContentBody(Payload payload, String kind) {
        return new HashMap<>() {{
            put("modelid", Base64.getEncoder().encode(payload.getModelId().getBytes()));
            put("uuid", Base64.getEncoder().encode(payload.getUUID().toString().getBytes()));
            if (payload.getData() != null) {
                ByteBuffer byteBuffer;
                try {
                    byteBuffer = payload.getData().nioBuffer();
                } catch (UnsupportedOperationException uoe) {
                    ByteBuf byteBuf = payload.getData();
                    final byte[] bytes = new byte[byteBuf.readableBytes()];
                    byteBuf.getBytes(byteBuf.readerIndex(), bytes);
                    byteBuffer = ByteBuffer.wrap(bytes);
                }
                put("data", Base64.getEncoder().encode(byteBuffer));
            } else {
                put("data", Base64.getEncoder().encode("".getBytes()));
            }
            put("kind", Base64.getEncoder().encode(kind.getBytes()));
        }};
    }

    private void sendPayload(Payload payload, Map<String, Object> values) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            byte[] requestBody = objectMapper
                    .writeValueAsBytes(values);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(requestBody))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                logger.warn("Processing {} didn't succeed: {}", payload, response);
            }
        } catch (Throwable e) {
            logger.error("An error occurred while sending payload {} to {}: {}", payload, uri, e.getMessage());
        }
    }

    @Override
    protected void processResponsePayload(Payload payload) {
        Map<String, Object> values = prepareContentBody(payload, "response");

        sendPayload(payload, values);
    }

    @Override
    public String getName() {
        return "remote";
    }

}
