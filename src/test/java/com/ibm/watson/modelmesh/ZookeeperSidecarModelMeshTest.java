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

import com.ibm.watson.zk.ZookeeperClient;
import org.apache.curator.test.TestingServer;

/**
 * Test use of ZK as the kv store
 */
public class ZookeeperSidecarModelMeshTest extends SidecarModelMeshTest {

    // Shared infrastructure
    private static TestingServer localZk;

    @Override
    protected String setupKvStore() throws Exception {
        localZk = setupZkServer();
        return "zookeeper:" + localZk.getConnectString();
    }

    @Override
    protected void tearDownKvStore() throws Exception {
        tearDownZkServer(localZk);
    }

    static TestingServer setupZkServer() throws Exception {
        // Local ZK setup
        TestingServer zk = new TestingServer();
        zk.start();
        return zk;
    }

    static void tearDownZkServer(TestingServer zk) throws Exception {
        ZookeeperClient.shutdown(false, false);
        if (zk != null) {
            zk.close();
        }
    }

}
