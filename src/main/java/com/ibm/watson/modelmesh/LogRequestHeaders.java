/*
 * Copyright 2022 IBM Corporation
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

package com.ibm.watson.modelmesh;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.type.MapLikeType;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.slf4j.MDC;

import java.util.Map;
import java.util.Map.Entry;

import static com.ibm.watson.modelmesh.ModelMeshEnvVars.LOG_REQ_HEADER_CONFIG_ENV_VAR;

/**
 * Logic related to configured gRPC metadata headers to include in log messages corresponding to incoming requests.
 * 
 * Configured by setting the MM_LOG_REQUEST_HEADERS environment variable to a json object like the following:
 *
 * <pre>
 * {
 *     "transaction_id": "txid",
 *     "user_id-bin": "user_id"
 * }
 * </pre>
 * 
 * The attribute names are the header names extract from the request (if present), the string values
 * are the map keys to use when inserting the corresponding key/value pairs into the log message context map (MDC).
 *
 * Note that header values are interpreted as ASCII unless the header name ends with "-bin", in which case
 * they are interpreted as Base64-encoded UTF-8.
 */
public final class LogRequestHeaders {
    private static final Logger logger = LogManager.getLogger(LogRequestHeaders.class);

    // Pairs (Key<String>, String) of headers where LHS is gRPC metadata header to log,
    // RHS is name of parameter to add to the logging thread context (MDC)
    private final Object[] logHeaders;

    public static LogRequestHeaders getConfiguredLogRequestHeaders() throws Exception {
        String logHeaderConfig = System.getenv(LOG_REQ_HEADER_CONFIG_ENV_VAR);
        return logHeaderConfig != null ? new LogRequestHeaders(logHeaderConfig) : null;
    }

    private LogRequestHeaders(String logHeaderConfig) throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        final MapLikeType logHeaderMapType = mapper.getTypeFactory()
                .constructMapLikeType(Map.class, String.class, String.class);
        try {
            Map<String, String> headerConfigMap = mapper.readValue(logHeaderConfig, logHeaderMapType);
            if (headerConfigMap == null || headerConfigMap.isEmpty()) {
                throw new Exception(LOG_REQ_HEADER_CONFIG_ENV_VAR + " env var provided with no entries");
            }
            logger.info("Request headers configured to log: " + headerConfigMap);
            Object[] headers = new Object[headerConfigMap.size() << 1];
            int i = 0;
            for (Entry<String, String> ent : headerConfigMap.entrySet()) {
                String key = ent.getKey();
                headers[i++] = key.endsWith("-bin") ? Key.of(key, GrpcSupport.UTF8_MARSHALLER)
                        : Key.of(key, Metadata.ASCII_STRING_MARSHALLER);
                headers[i++] = ent.getValue();
            }
            logHeaders = headers;
        } catch (Exception e) {
            throw new Exception("Error parsing json from " + LOG_REQ_HEADER_CONFIG_ENV_VAR + " env var", e);
        }
    }

    public void addToMDC(Metadata headers) {
        for (int i = 0; i < logHeaders.length; i += 2) {
            String value = headers.get((Key<String>) logHeaders[i]);
            if (value != null) {
                MDC.put((String) logHeaders[i + 1], value);
            }
        }
    }

    public ServerInterceptor serverInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                try {
                    addToMDC(headers);
                    final Listener<ReqT> original = next.startCall(call, headers);
                    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(original) {
                        @Override
                        public void onMessage(final ReqT message) {
                            try {
                                addToMDC(headers);
                                super.onMessage(message);
                            } finally {
                                MDC.clear();
                            }
                        }
                        @Override
                        public void onHalfClose() {
                            try {
                                addToMDC(headers);
                                super.onHalfClose();
                            } finally {
                                MDC.clear();
                            }
                        }
                        @Override
                        public void onCancel() {
                            try {
                                addToMDC(headers);
                                super.onCancel();
                            } finally {
                                MDC.clear();
                            }
                        }
                        @Override
                        public void onComplete() {
                            try {
                                addToMDC(headers);
                                super.onComplete();
                            } finally {
                                MDC.clear();
                            }
                        }
                        @Override
                        public void onReady() {
                            try {
                                addToMDC(headers);
                                super.onReady();
                            } finally {
                                MDC.clear();
                            }
                        }
                    };
                } finally {
                    MDC.clear();
                }
            }
        };
    }

}
