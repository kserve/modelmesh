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

import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * JSON schema for definitions in static model registration configuration.
 * The config is provided as a JSON list of these objects.
 */
public class StaticModelDefiniton {

    static class ModelInfo {
        private String type; // required
        private String path = ""; // default empty
        private String key = ""; // default empty

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        @Override
        public String toString() {
            return "{type=" + type + (isNullOrEmpty(path) ? "" : ", path=" + path)
                   + (isNullOrEmpty(key) ? "" : ", key=<hidden>") + "}";
        }
    }

    private String modelId; // required
    private ModelInfo modelInfo; // required

    private String vModelId; // optional

    // applies only if vModelId specified
    private boolean retainAfterTransition;

    private boolean ensureLoaded; // default is false

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public ModelInfo getModelInfo() {
        return modelInfo;
    }

    public void setModelInfo(ModelInfo modelInfo) {
        this.modelInfo = modelInfo;
    }

    public boolean isEnsureLoaded() {
        return ensureLoaded;
    }

    public void setEnsureLoaded(boolean ensureLoaded) {
        this.ensureLoaded = ensureLoaded;
    }

    public String getvModelId() {
        return vModelId;
    }

    public void setvModelId(String vModelId) {
        this.vModelId = vModelId;
    }

    public boolean isRetainAfterTransition() {
        return retainAfterTransition;
    }

    public void setRetainAfterTransition(boolean retainAfterTransition) {
        this.retainAfterTransition = retainAfterTransition;
    }

    @Override
    public String toString() {
        return "[modelId=" + modelId + ", modelInfo=" + modelInfo + ", vModelId="
               + (vModelId != null ? vModelId + " (retain=" + retainAfterTransition + ")" : "<none>")
               + ", ensureLoaded=" + ensureLoaded + "]";
    }
}
