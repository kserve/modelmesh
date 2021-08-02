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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.ibm.watson.modelmesh.thrift.ModelInfo;

/**
 * This interface is implemented per model "type". The ModelLoader
 * for a given model type is responsible for loading the model into
 * memory and providing an runtime instance which has the methods
 * used to invoke that model type.
 * <p>
 * It's also responsible for providing size predictions and
 * calculations for the loaded models.
 *
 * @param <T> the loaded runtime instance, implementing the appropriate
 *            model application methods
 */
public abstract class ModelLoader<T> {
    public static final long UNIT_SIZE = 8192; // 8k

    private static final String UNLOAD_METHOD_NAME = "unloadModel";
    private static final String UNLOAD_METHOD_NAME2 = "unloadRuntime";

    protected static final ListenableFuture<Boolean> COMPLETED = Futures.immediateFuture(Boolean.TRUE);
    protected static final ListenableFuture<Void> VOID_COMPLETED = Futures.immediateFuture(null);

    private final boolean requiresUnload;

    public ModelLoader() {
        boolean ul = false;
        try {
            getClass().getDeclaredMethod(UNLOAD_METHOD_NAME, String.class);
            ul = true;
        } catch (NoSuchMethodException nsme) {
            try {
                getClass().getDeclaredMethod(UNLOAD_METHOD_NAME2, String.class);
                ul = true;
            } catch (NoSuchMethodException nsme2) { }
        }
        requiresUnload = ul;
    }

    boolean requiresUnload() {
        return requiresUnload;
    }

    public static int toUnits(long bytes) {
        // round up to nearest # units
        int units = Math.toIntExact(bytes / UNIT_SIZE);
        return bytes % UNIT_SIZE == 0 ? units : units + 1;
    }

    /**
     * @param modelId
     * @param modelInfo
     * @return predicted size of model in units of size {@link ModelLoader#UNIT_SIZE} bytes
     */
    public abstract int predictSize(String modelId, ModelInfo modelInfo);

    /**
     * @param runtime
     * @return actual size of loaded runtime in units of size {@link ModelLoader#UNIT_SIZE} bytes
     */
    public abstract int modelSize(T runtime) throws Exception;

    /**
     * Load the specified model and return the corresponding runtime instance
     *
     * @param modelId
     * @param modelInfo
     * @return
     * @throws Exception
     */
    public abstract LoadedRuntime<T> loadRuntime(String modelId, ModelInfo modelInfo) throws Exception;

    /**
     * Asynchronously unload model with specified id
     * <p>
     * This method <b>should not</b> be overridden if no custom unloading logic is required
     * (i.e. GC only)
     *
     * @param modelId
     * @deprecated use {@link #unloadModel(String)}
     */
    @Deprecated
    public ListenableFuture<Void> unloadRuntime(String modelId) {
        return VOID_COMPLETED; // default is no-op
    }

    /**
     * Asynchronously unload model with specified id
     * <p>
     * This method <b>should not</b> be overridden if no custom unloading logic is required
     * (i.e. GC only)
     *
     * @param modelId
     * @return false if prior load was not performed due to an existing failed load attempt
     */
    public ListenableFuture<Boolean> unloadModel(String modelId) {
        return Futures.transform(unloadRuntime(modelId), v -> Boolean.TRUE, MoreExecutors.directExecutor());
    }

    public static class LoadedRuntime<T> {
        private final int size;
        private final int maxConcurrency;
        private final T runtime;

        public LoadedRuntime(T runtime) {
            this(runtime, 0, 0);
        }

        public LoadedRuntime(T runtime, int size) {
            this(runtime, size, 0);
        }

        public LoadedRuntime(T runtime, int size, int maxConcurrency) {
            this.runtime = runtime;
            this.size = size;
            this.maxConcurrency = maxConcurrency;
        }

        public T getRuntime() {
            return runtime;
        }

        /**
         * @return optional - return 0 if sizing has nontrivial cost
         * associated, in which case separate {@link ModelLoader#modelSize(Object)}
         * method is used to perform the sizing
         */
        public int getSizeInUnits() {
            return size;
        }

        public int getMaxConcurrency() {
            return maxConcurrency;
        }
    }
}
