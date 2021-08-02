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


/*
 * This interface is used only in some of the older unit tests.
 * It will go away once they are refactored.
 *
 * thrift --gen java:generated_annotations=undated,reuse-objects,unsafe_binaries -out src/main/java src/main/thrift/modelmesh-legacy.thrift
 */

include "modelmesh.thrift"

namespace java com.ibm.watson.modelmesh.thrift


// Deprecated
struct ModelInfo {
    1: string serviceType,
    2: string modelPath,
    3: optional string encKey
}

// Deprecated
exception InvalidInputException {
    1: string message
}

// Deprecated
exception InvalidStateException {
    1: string message
}

// Used only by old unit tests
service LegacyModelMeshService extends modelmesh.ModelMeshService {
    /**
     * Adds a trained model to this MM cluster.
     */
    modelmesh.StatusInfo addModel(
        /** the id of the model to add */
        1: string modelId,
        /** information required to load/serve the model, including the type */
        2: ModelInfo runtimeInfo,
        /** whether the model should be loaded immediately */
        3: bool load,
        /** if load is true, whether this method should block until the load completes */
        4: bool sync)
        throws (
            /** invalid value provided for parameter or inconsistent with existing model */
            1:InvalidInputException iiException,
            /** unexpected internal errors */
            2:modelmesh.InternalException iException)

    /**
     * Ensures the model with the specified id is loaded in this MM cluster.
     */
    modelmesh.StatusInfo ensureLoaded(1: string modelId,
        /** the timestamp associated with the load, or 0 for "now" (most typical) */
        2: i64 lastUsed,
        /** optional list of instance ids to exclude - if the model is already loaded in one of these */
        3: list<string> excludeInstances,
        /* true if this method call should block until the model load is complete */
        4: bool sync,
        /** whether an accurate loading status should be returned (this parameter might be removed) */
        5: bool getStatus)
        throws (
            /** this MM cluster doesn't know about a model with the specified id */
            1: modelmesh.ModelNotFoundException mnfException,
            /** unexpected internal errors */
            2: modelmesh.InternalException iException)

    /**
     * Removes the model with the specified id from this MM cluster. Has no effect if the specified model isn't found
     */
    void deleteModel(1: string modelId) throws (1:modelmesh.InternalException iException, 2:InvalidStateException isException)

    /**
     * Returns the status of the specified model. See the {@link Status} enum.
     */
    modelmesh.StatusInfo getStatus(1: string modelId) throws (1:modelmesh.InternalException iException)
}
