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

import com.ibm.watson.modelmesh.DummyModelMesh.DummyModel;
import com.ibm.watson.modelmesh.thrift.ModelInfo;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DummyClassifierLoader extends ModelLoader<DummyModel> {
    /* This is something of a hack to allow us to control
     * behavior of an arbitrary TAS instance's ModelLoader
     * during unit testing.
     *
     * We (possibly) store a config map for each TAS
     * instance within the JVM, keyed by the Litelinks
     * instance ID.
     *
     * When modelSize and loadRuntime are called by an
     * instance of DummyClassifierLoader, they will first
     * grab their config map (if it exists) and check for
     * relevant parameters, defaulting to to the presets
     * below.
     */
    private static final Map<String, Map<String, String>>
            loaderInstanceConfigMap = new ConcurrentHashMap<>();

    public static final String FORCE_FAIL_KEY = "forceFail"; // default: false
    public static final String LOAD_TIME_KEY = "loadTime";
    public static final String MODEL_SIZE_KEY = "modelSize";

    public static final long DEFAULT_LOAD_TIME = 1_000L;
    public static final int DEFAULT_MODEL_SIZE = Math.toIntExact(20 * 1024 * 1024 / UNIT_SIZE);

    private String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public int predictSize(String modelId, ModelInfo modelInfo) {
        return DEFAULT_MODEL_SIZE;
    }

    @Override
    public int modelSize(DummyModel runtime) {
        // Get config
        Map<String, String> config = loaderInstanceConfigMap.get(id);
        int modelSize = DEFAULT_MODEL_SIZE;
        if (config != null) {
            String modelSizeVal = config.get(MODEL_SIZE_KEY);
            if (modelSizeVal != null) {
                modelSize = Integer.parseInt(modelSizeVal);
            }
        }
        return modelSize;
    }

    @Override
    public LoadedRuntime<DummyModel> loadRuntime(final String modelId, ModelInfo modelInfo) throws Exception {
        // Get config
        Map<String, String> config = loaderInstanceConfigMap.get(id);
        boolean forceFail = false;
        long loadTime = DEFAULT_LOAD_TIME;
        System.out.println("[Loader] config:" + config);
        if (config != null) {
            String forceFailVal = config.get(FORCE_FAIL_KEY);
            System.out.println(
                    "[Loader] forceFailKey:" + config.containsKey(FORCE_FAIL_KEY) + " forceFailVal:" + forceFailVal);
            if (forceFailVal != null) {
                forceFail = "true".equals(forceFailVal);
            }
            String loadTimeVal = config.get(LOAD_TIME_KEY);
            if (loadTimeVal != null) {
                loadTime = Long.parseLong(loadTimeVal);
            }
        }

        System.out.println("[Loader] Dummy loader about to start loading model " + modelId);
        loaderInstanceConfigMap.get(id);
        Thread.sleep(loadTime + (long) (200 * Math.random()));

        if (forceFail) {
            throw new Exception("[Loader] Dummy loader forced failure for model " + modelId);
        }
        System.out.println("[Loader] Dummy loader finished loading model " + modelId);
        return new LoadedRuntime<>(new DummyModel() {
            @Override
            public ByteBuffer applyModel(ByteBuffer input, Map<String, String> metadata) throws Exception {
                return ByteBuffer.wrap(("[Loader] Reply from " + modelId + " in " + id).getBytes());
            }
        });
    }

    public static void updateLoaderInstanceConfig(String id, String key, String value) {
        Map<String, String> config = loaderInstanceConfigMap.get(id);
        if (config != null) {
            config.put(key, value);
        } else {
            config = new HashMap<>();
            config.put(key, value);
            loaderInstanceConfigMap.put(id, config);
        }
    }

    public static void updateLoaderInstanceConfig(String id, Map<String, String> config) {
        loaderInstanceConfigMap.put(id, config);
    }

//    @Override
//    public ListenableFuture<Void> unloadRuntime(String modelId) {
//        System.out.println("unloading called for "+modelId);
//        return super.unloadRuntime(modelId);
//    }
}
