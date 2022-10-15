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
 * This interface is now used only for intra-instance communication
 *
 * Thrift compiler command used to generate stubs:
 * thrift --gen java:generated_annotations=undated,reuse-objects,unsafe_binaries -out src/main/java src/main/thrift/modelmesh.thrift
 */

namespace java com.ibm.watson.modelmesh.thrift

enum Status {
    NOT_FOUND,
    NOT_LOADED,
    LOADING,
    LOADED,
    LOADING_FAILED,
    NOT_CHECKED // this one still TBC
}

struct StatusInfo {
    1: required Status status,
    2: list<string> errorMessages
}

exception ModelNotHereException {
    1: string instanceId,
    2: string modelId
}

exception ModelNotFoundException {
    1: string modelId
}

exception ModelLoadException {
    1: string message,
    2: string instanceId,
    /** if non-zero then this was a timeout */
    3: i64 timeout,
    4: string causeStacktrace
}

exception InternalException {
    1: string message,
    2: string causeStacktrace
}


/**
  *
  */
service BaseModelMeshService {

    /** Used for internal forwarding of getStatus and ensureLoaded calls between instances */
    StatusInfo internalOperation(1: string modelId, 2: bool getStatus, 3: bool load,
                                 4: bool sync, 5: i64 lastUsed, 6: list<string> excludeInstances)
        throws (1: ModelNotFoundException mnfException, 2: ModelLoadException mlException,
                3: ModelNotHereException notHere, 4: InternalException iException)
}


exception ApplierException {
    1: string message,
    2: string causeStacktrace,
    3: string grpcStatusCode
}

struct ApplierResponse {
    1: binary result,
    2: binary metadata
}

service ModelMeshService extends BaseModelMeshService {

    /** Used for internal forwarding of inferencing request/response between instances */
    list<binary> applyModelMulti(1: string modelId, 2: list<binary> input, 3: map<string,string> metadata)
        throws (1:ApplierException applierException, 2:ModelNotHereException notHere,
            3: ModelNotFoundException mnfException, 4: ModelLoadException mlException, 5: InternalException iException)


    // Deprecated - we now chain instead of concatenate/copy ByteBuffers
    binary applyModel(1: string modelId, 2: binary input, 3: map<string,string> metadata)
        throws (1:ApplierException applierException, 2:ModelNotHereException notHere,
                3: ModelNotFoundException mnfException, 4: ModelLoadException mlException, 5: InternalException iException)
}
