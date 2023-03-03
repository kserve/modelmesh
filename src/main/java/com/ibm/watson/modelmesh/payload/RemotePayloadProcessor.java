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
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;

import java.util.Base64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PayloadProcessor} that sends payloads to a remote service via HTTP POST.
 */
public class RemotePayloadProcessor implements PayloadProcessor {

    private final static Logger logger = LoggerFactory.getLogger(RemotePayloadProcessor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final URI uri;
    private final HttpClient client;

    public RemotePayloadProcessor(URI uri) {
        this.uri = uri;
        this.client = HttpClient.newHttpClient();
    }

    @Override
    public void process(Payload payload) {
        Map<String, Object> values = prepareContentBody(payload);
        sendPayload(payload, values);
    }

    private static Map<String, Object> prepareContentBody(Payload payload) {
        return new HashMap<>() {{
            put("modelid", payload.getModelId());
            put("id", payload.getId());
            if (payload.getData() != null) {
                ByteBuf byteBuf = payload.getData();
                final byte[] bytes = new byte[byteBuf.readableBytes()];
                byteBuf.getBytes(0, bytes);
                put("data", Base64.getEncoder().encodeToString(bytes));
            } else {
                put("data", "");
            }
            put("kind", payload.getKind());
        }};
    }

    private void sendPayload(Payload payload, Map<String, Object> values) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .headers("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(values)))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                logger.warn("Processing {} with request {} didn't succeed: {}", payload, values, response);
            }
        } catch (Throwable e) {
            logger.error("An error occurred while sending payload {} to {}: {}", payload, uri, e.getMessage());
        }
    }

    @Override
    public String getName() {
        return "remote";
    }
}
