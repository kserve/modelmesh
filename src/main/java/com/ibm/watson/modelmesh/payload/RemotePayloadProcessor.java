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
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Metadata;
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
        return sendPayload(payload);
    }

    private static PayloadContent prepareContentBody(Payload payload) {
        String id = payload.getId();
        String modelId = payload.getModelId();
        String kind = payload.getKind().toString().toLowerCase();
        ByteBuf byteBuf = payload.getData();
        String data;
        if (byteBuf != null) {
            data = encodeBinaryToString(byteBuf);
        } else {
            data = "";
        }
        Metadata metadata = payload.getMetadata();
        Map<String, String> metadataMap = new HashMap<>();
        if (metadata != null) {
            for (String key : metadata.keys()) {
                if (key.endsWith("-bin")) {
                    byte[] bytes = metadata.get(Metadata.Key.of(key, Metadata.BINARY_BYTE_MARSHALLER));
                    metadataMap.put(key, java.util.Base64.getEncoder().encodeToString(bytes));
                } else {
                    String value = metadata.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
                    metadataMap.put(key, value);
                }
            }
        }
        String status = payload.getStatus() != null ? payload.getStatus().getCode().toString() : "";
        return new PayloadContent(id, modelId, data, kind, status, metadataMap);
    }

    private static String encodeBinaryToString(ByteBuf byteBuf) {
        ByteBuf encodedBinary = Base64.encode(byteBuf, byteBuf.readerIndex(), byteBuf.readableBytes(), false);
        //TODO custom jackson serialization for this field to avoid round-tripping to string
        return encodedBinary.toString(StandardCharsets.US_ASCII);
    }

    private boolean sendPayload(Payload payload) {
        try {
            PayloadContent payloadContent = prepareContentBody(payload);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .headers("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payloadContent)))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                logger.warn("Processing {} with request {} didn't succeed: {}", payload, payloadContent, response);
            }
        } catch (Throwable e) {
            logger.error("An error occurred while sending payload {} to {}: {}", payload, uri, e.getCause());
        }
        return false;
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
        private final String status;
        private final Map<String, String> metadata;

        private PayloadContent(String id, String modelid, String data, String kind, String status,
                               Map<String, String> metadata) {
            this.id = id;
            this.modelid = modelid;
            this.data = data;
            this.kind = kind;
            this.status = status;
            this.metadata = metadata;
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

        public String getStatus() {
            return status;
        }

        public Map<String, String> getMetadata() {
            return metadata;
        }

        @Override
        public String toString() {
            return "PayloadContent{" +
                    "id='" + id + '\'' +
                    ", modelid='" + modelid + '\'' +
                    ", data='" + data + '\'' +
                    ", kind='" + kind + '\'' +
                    ", status='" + status + '\'' +
                    ", metadata='" + metadata + '\'' +
                    '}';
        }
    }
}
