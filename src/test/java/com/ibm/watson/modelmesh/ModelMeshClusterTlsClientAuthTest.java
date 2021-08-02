/*
 * Copyright 2021 IBM Corporation
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

import com.google.common.collect.ImmutableMap;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class ModelMeshClusterTlsClientAuthTest extends ModelMeshClusterTlsTest {

    static final String CLIENT_KEY, CLIENT_CERT;
    static {
        final Class<?> C = ModelMeshClusterTlsTest.class;
        CLIENT_KEY = C.getResource("/certs/key2.pem").getFile();
        CLIENT_CERT = C.getResource("/certs/cert2.pem").getFile();
    }

    @Override
    protected Map<String, String> extraEnvVars() {
        return ImmutableMap.<String, String>builder().putAll(super.extraEnvVars())
                .put(ModelMeshEnvVars.TLS_TRUST_CERT_PATH_ENV_VAR, CLIENT_CERT)
                .put(ModelMeshEnvVars.TLS_CLIENT_AUTH_ENV_VAR, "require").build();
    }

    @Test
    public void testTlsClientAuthGrpcConnection() throws Exception {
        testGrpcConnection(NettyChannelBuilder.forAddress("localhost", 9000)
                .sslContext(GrpcSslContexts.forClient()
                        .trustManager(new File(SERVER_CERT))
                        .keyManager(new File(CLIENT_CERT), new File(CLIENT_KEY)).build())
                .overrideAuthority("litelinks-test").build());
    }

    @Override
    @Test
    public void testTlsGrpcConnection() throws Exception {
        try {
            super.testTlsGrpcConnection();
            fail("unauthenticated TLS conn should fail");
        } catch (StatusRuntimeException sre) {
            System.out.println("Unauthenticated attempt failed: " + sre);
            assertEquals(Code.UNAVAILABLE, sre.getStatus().getCode());
        }
    }
}
