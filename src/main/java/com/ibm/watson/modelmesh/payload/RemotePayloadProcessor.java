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
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.base64.Base64;
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
    public boolean process(Payload payload) {
        return sendPayload(payload, prepareContentBody(payload));
    }

    private static PayloadContent prepareContentBody(Payload payload) {
        String id = payload.getId();
        String modelId = payload.getModelId();
        String kind = payload.getKind().toString();
        ByteBuf byteBuf = payload.getData();
        String data;
        if (byteBuf != null) {
            ByteBuf encoded = Base64.encode(byteBuf, byteBuf.readerIndex(), byteBuf.readableBytes());
            //TODO custom jackson serialization for this field to avoid round-tripping to string
            data = encoded.toString(StandardCharsets.UTF_8);
        } else {
            data = "";
        }
        return new PayloadContent(id, modelId, data, kind);
    }

    private boolean sendPayload(Payload payload, PayloadContent values) {
        boolean sent = false;
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .headers("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(values)))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                logger.warn("Processing {} with request {} didn't succeed: {}", payload, values, response);
            } else {
                sent = true;
            }
        } catch (Throwable e) {
            logger.error("An error occurred while sending payload {} to {}: {}", payload, uri, e.getCause());
        }
        return sent;
    }

    @Override
    public String getName() {
        return "remote";
    }

    private static class PayloadContent {
        private final String id;
        private final String modelid;
        private final String data;
        private final String kind;

        private PayloadContent(String id, String modelid, String data, String kind) {
            this.id = id;
            this.modelid = modelid;
            this.data = data;
            this.kind = kind;
        }

        public String getId() {
            return id;
        }

        public String getKind() {
            return kind;
        }

        public String getModelid() {
            return modelid;
        }

        public String getData() {
            return data;
        }
    }
}
