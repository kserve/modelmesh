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

import java.util.Collections;
import java.util.Map;

/**
 * Test recovery behaviour where model runtime unexpectedly returns NOT_FOUND from an inference request
 * (typically because it restarted).
 *
 * This verifies handling of the Nvidia Triton Inference Server's misleading response code
 * of UNAVAILABLE in the not found case, see https://github.com/triton-inference-server/server/issues/3399
 */
public class ModelMeshTritonRefreshMissingModelTest extends ModelMeshRefreshMissingModelTest {

    @Override
    protected Map<String, String> extraRuntimeEnvVars() {
        return Collections.singletonMap("TRITON_NOT_FOUND_BEHAVIOUR", "true");
    }
}
