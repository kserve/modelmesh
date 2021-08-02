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

import io.netty.channel.epoll.Epoll;
import org.junit.jupiter.api.BeforeAll;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Train-and-serve runtime service unit tests
 */
public class UdsSidecarModelMeshTest extends SidecarModelMeshTest {

    @BeforeAll
    public static void setup() {
        assumeTrue(Epoll.isAvailable());
    }

    // using default domain socket path
    //System.setProperty(SidecarModelMesh.GRPC_UDS_PATH_ENV_VAR, "/tmp/mmesh/grpc.sock");

    //System.clearProperty(SidecarModelMesh.GRPC_UDS_PATH_ENV_VAR);

    @Override
    protected String getModelRuntimeEndpointString() {
        return "unix:///tmp/mmesh/grpc.sock";
    }

}
