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

public interface PayloadProcessor extends Closeable {

    String getName();

    /**
     * If this returns false then {@link #process(Payload)} should never return true
     */
    default boolean mayTakeOwnership() {
        return false;
    }

    /**
     * @param payload the indices of any contained byte buffers should not be changed
     * @return true if the called method took ownership of the
     *    payload, false otherwise.
     */
    boolean process(Payload payload);

}
