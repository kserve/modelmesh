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

import com.google.common.util.concurrent.Service;
import com.ibm.watson.kvutils.factory.KVUtilsFactory;
import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import com.ibm.watson.litelinks.server.LitelinksService;
import com.ibm.watson.litelinks.server.LitelinksService.ServiceDeploymentConfig;
import com.ibm.watson.modelmesh.example.ExampleModelRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Model-mesh unit tests
 */
public abstract class SingleInstanceModelMeshTest extends AbstractModelMeshTest {

    private static final String standaloneServiceName = "model-mesh-test";

    static Service mmService;
    static ExampleModelRuntime exampleRuntime;

    @BeforeEach
    public void initialize() throws Exception {
        //shared infrastructure
        String kvStoreString = setupKvStore();
        System.setProperty(LitelinksSystemPropNames.SERVER_REGISTRY, kvStoreString);
        System.setProperty(KVUtilsFactory.KV_STORE_EV, kvStoreString);

        // Create standalone MM service
        System.setProperty(ModelMeshEnvVars.GRPC_PORT_ENV_VAR, "8088");
        mmService = LitelinksService.createService(
                new ServiceDeploymentConfig(SidecarModelMesh.class)
                        .setServiceRegistry(kvStoreString)
                        .setServiceName(standaloneServiceName)
                        .setServiceVersion("20170315-1347"));

        System.out.println("starting model-mesh server");
        mmService.startAsync();

        String listenOn = getModelRuntimeEndpointString();
        System.out.println("starting example runtime server listening on " + listenOn);
        exampleRuntime = new ExampleModelRuntime(listenOn, 0).start();

        System.out.println("waiting for model-mesh start");
        mmService.awaitRunning();
        System.out.println("model-mesh start complete");
        System.clearProperty(ModelMeshEnvVars.GRPC_PORT_ENV_VAR);
    }

    protected String getModelRuntimeEndpointString() {
        return "8085"; // default port expected by model-mesh
    }

    @AfterEach
    public void shutdown() throws Exception {
        System.out.println("stopping modelmesh server");
        mmService.stopAsync().awaitTerminated();

        System.out.println("modelmesh server stop complete, stopping runtime server");
        exampleRuntime.shutdown().awaitTermination();
        System.out.println("runtime server stop complete");

        KVUtilsFactory.resetDefaultFactory();
        tearDownKvStore();
    }
}
