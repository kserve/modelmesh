/*
 * Copyright 2023 IBM Corporation
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

package com.ibm.watson.modelmesh.payload;

import java.io.Closeable;
import java.io.IOException;

/**
 * A {@link PayloadProcessor} is responsible for processing {@link Payload}s for models served by model-mesh.
 * Processing shall not modify/dispose payload data.
 */
public interface PayloadProcessor extends Closeable {

    /**
     * Get this processor name.
     *
     * @return the processor name.
     */
    String getName();

    /**
     * Check whether this processor may take ownership (e.g., retaining payload data).
     * If this returns {@code false} then {@link #process(Payload)} should never return {@code true}.
     */
    default boolean mayTakeOwnership() {
        return false;
    }

    /**
     * Process a payload.
     * The indices of any contained byte buffers should not be changed
     *
     * @param payload the payload to be processed.
     * @return {@code true} if the called method took ownership of the payload, {@code false} otherwise.
     */
    boolean process(Payload payload);

    @Override
    default void close() throws IOException {}
}
