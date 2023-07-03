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

import com.ibm.watson.litelinks.server.Idempotent;
import com.ibm.watson.modelmesh.thrift.LegacyModelMeshService;
import com.ibm.watson.modelmesh.thrift.ModelMeshService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * For unit tests
 */
public class DummyModelMesh extends ModelMesh implements LegacyModelMeshService.Iface {
    private static final Logger logger = LogManager.getLogger(DummyModelMesh.class);

    public static final String DUMMY = "DUMMY";

    public static final long DUMMY_CAPACITY = 10 * DummyClassifierLoader.DEFAULT_MODEL_SIZE * ModelLoader.UNIT_SIZE;

    public static class DummyModel {
        int size;
        DummyModel(int size) {
            this.size = size;
        }
    
        public ByteBuffer applyModel(ByteBuffer input, Map<String, String> metadata) throws Exception {
            return ByteBuffer.wrap(new byte[5]);
        }
    }

    private /*final*/ ModelLoader<DummyModel> dummyLoader;

    public DummyModelMesh() throws Exception {
    }

    @Override
    protected LocalInstanceParameters startup() throws Exception {
        logger.info("DummyTasRuntime starting up...");
        dummyLoader = new DummyClassifierLoader();
        ((DummyClassifierLoader) dummyLoader).setId(instanceId);
        return new LocalInstanceParameters(DummyClassifierLoader.DEFAULT_MODEL_SIZE)
                .useNonHeap(DUMMY_CAPACITY).acceptedModelTypes(DUMMY);
    }

    @Override
    protected ModelLoader<?> getLoader() {
        return dummyLoader;
    }

    protected static final Method localMeth = getMethod(DummyModel.class, "applyModel");
    protected static final Method remoteMeth = getMethod(ModelMeshService.Iface.class, "applyModel");

    @Idempotent
    @Override
    public ByteBuffer applyModel(String modelId, ByteBuffer input, Map<String, String> metadata)
            throws TException {
        return (ByteBuffer) invokeModel(modelId, localMeth, remoteMeth, input, metadata);
    }

    @Override
    protected void shutdown() {
        logger.info("DummyTasRuntime shutting down...");

        super.shutdown();
    }

    @Override
    public List<ByteBuffer> applyModelMulti(String modelId, List<ByteBuffer> input, Map<String, String> metadata)
            throws TException {
        throw new UnsupportedOperationException(); // not used by this test
    }

}
