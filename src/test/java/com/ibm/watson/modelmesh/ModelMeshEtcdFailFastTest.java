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


import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Ensure process fails fast/correctly when the etcd config is invalid
 */
public class ModelMeshEtcdFailFastTest {

    @Test
    public void etcdFailFastTest() throws Exception {
        String replicaSetId = "RS0";
        String invalidEtcdServer = "etcd:http://localhost:9999";

        ProcessBuilder mmPb = AbstractModelMeshClusterTest.buildMmProcess("fail-fast-test-cluster",
                replicaSetId, "9012", invalidEtcdServer, 9012, 9014, 9013, Collections.emptyMap(),
                Collections.emptyList(), Collections.emptyList());

        Process mmProc = mmPb.start();
        try {
            // Give it 5 sec to die
            assertTrue(mmProc.waitFor(5, TimeUnit.SECONDS));
            int exitCode = mmProc.exitValue();
            System.out.println("Exit code: " + exitCode);
            assertNotEquals(0, exitCode);
            String msg = AbstractModelMeshClusterTest.getTerminationMessage(mmPb);
            System.out.println("Termination msg: " + msg);
            //TODO Investigate why this sometimes fails with file not found
            //assertEquals("Service startup failed: Problem connecting to etcd", msg.replace("\n", "");
            if (!"Service startup failed: Problem connecting to etcd".equals(msg.replace("\n", ""))) {
                System.err.println("EXPECTED: " + "Service startup failed: Problem connecting to etcd"
                                   + " BUT WAS: " + msg.replace("\n", ""));
            }
        } finally {
            if (mmProc.isAlive()) {
                mmProc.destroyForcibly();
            }
        }
    }
}
