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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.ibm.watson.modelmesh.api.GetStatusRequest;
import com.ibm.watson.modelmesh.api.ModelMeshGrpc;
import com.ibm.watson.modelmesh.api.ModelStatusInfo.ModelStatus;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ModelMeshClusterTlsTest extends AbstractModelMeshClusterTest {

    @Override
    protected int replicaCount() {
        return 1;
    }

    static final String SERVER_KEY, SERVER_CERT;
    static {
        final Class<?> C = ModelMeshClusterTlsTest.class;
        SERVER_KEY = C.getResource("/certs/key.pem").getFile();
        SERVER_CERT = C.getResource("/certs/cert.pem").getFile();
    }

    @Override
    protected Map<String, String> extraEnvVars() {
        return ImmutableMap.of(
                ModelMeshEnvVars.TLS_PRIV_KEY_PATH_ENV_VAR, SERVER_KEY,
                ModelMeshEnvVars.TLS_KEY_CERT_PATH_ENV_VAR, SERVER_CERT);
    }

    @Override
    protected List<String> extraLitelinksArgs() {
        return Arrays.asList("-e", "ssl-ca");
    }

    @Override
    protected List<String> extraJvmArgs() {
        return Arrays.asList(
                "-Dlitelinks.ssl.key.path=" + SERVER_KEY,
                "-Dlitelinks.ssl.key.certpath=" + SERVER_CERT,
                "-Dlitelinks.ssl.trustcerts.path=" + SERVER_CERT);
    }

    @Test
    public void testTlsGrpcConnection() throws Exception {
        testGrpcConnection(NettyChannelBuilder.forAddress("localhost", 9000)
                .sslContext(GrpcSslContexts.forClient().trustManager(new File(SERVER_CERT)).build())
                .overrideAuthority("litelinks-test").build());
    }

    protected void testGrpcConnection(ManagedChannel channel) throws Exception {
        try {
            // just see if we can connect
            assertEquals(ModelStatus.NOT_FOUND, ModelMeshGrpc.newBlockingStub(channel)
                    .getModelStatus(GetStatusRequest.newBuilder()
                            .setModelId("i don't exist").build()).getStatus());
        } finally {
            channel.shutdown();
        }
    }
}
