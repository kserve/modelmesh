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

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 *
 */
public class ModelMeshAllowWithNoInjectionTest extends ModelMeshAllowAnyMethodTest {

    @Override
    protected Map<String, String> extraRuntimeEnvVars() {
        return ImmutableMap.of("RS_METHOD_INFOS",
                "mmesh.ExamplePredictor/predict=1;"
                // empty path array for second method - whitelist it without
                // doing any id injection
                + "mmesh.ExamplePredictor/multiPredict=");
    }

}
