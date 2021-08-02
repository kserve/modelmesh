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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static com.ibm.watson.modelmesh.ModelMesh.getStringParameter;
import static com.ibm.watson.modelmesh.ModelMeshEnvVars.DATAPLANE_CFG_ENV_VAR;
import static com.ibm.watson.modelmesh.ModelMeshEnvVars.DATAPLANE_CFG_FILE_ENV_VAR;

/**
 * Manages loading dataplane API configuration
 */
public class DataplaneApiConfig {
    private static final Logger logger = LogManager.getLogger(DataplaneApiConfig.class);

    /**
     * The ApiConfig and RpcConfig define the JSON schema for the
     * configuration and must not be changed in a backwards-incompatible way.
     */
    public static class ApiConfig {
        private final Map<String, RpcConfig> rpcConfigs;
        private final boolean allowOtherRpcs;

        public ApiConfig(Map<String, RpcConfig> rpcConfigs, boolean allowOtherRpcs) {
            this.rpcConfigs = rpcConfigs;
            this.allowOtherRpcs = allowOtherRpcs;
        }

        public ApiConfig() {
            this(Collections.emptyMap(), false);
        }

        @JsonProperty("rpcConfigs")
        public Map<String, RpcConfig> getRpcConfigs() {
            return rpcConfigs;
        }

        @JsonProperty("allowOtherRpcs")
        public boolean isAllowOtherRpcs() {
            return allowOtherRpcs;
        }

        /**
         * @return null if rpc is disallowed
         */
        @JsonIgnore
        public RpcConfig getRpcConfig(String fullRpcName) {
            RpcConfig rpcc = rpcConfigs.get(fullRpcName);
            return rpcc != null ? rpcc : (allowOtherRpcs ? RpcConfig.DEFAULT : null);
        }

        @Override
        public String toString() {
            return "[rpcConfigs=" + rpcConfigs + ", allowOtherRpcs=" + allowOtherRpcs + "]";
        }
    }

    /**
     * The ApiConfig and RpcConfig define the JSON schema for the
     * configuration and must not be changed in a backwards-incompatible way.
     */
    public static class RpcConfig {
        private static final int[] EMPTY_PATH = new int[0];
        private static final RpcConfig DEFAULT = new RpcConfig();

        private final int[] idExtractionPath;
        private final boolean vModelId;

        public RpcConfig(int[] idExtractionPath, boolean vmodelId) {
            this.idExtractionPath = Objects.requireNonNull(idExtractionPath);
            this.vModelId = vmodelId;
        }

        public RpcConfig() {
            this(EMPTY_PATH, false);
        }

        @JsonProperty("idExtractionPath")
        public int[] getIdExtractionPath() {
            return idExtractionPath;
        }

        @JsonProperty("vModelId")
        public boolean isvModelId() {
            return vModelId;
        }

        public boolean shouldExtract() {
            return idExtractionPath.length != 0;
        }

        @Override
        public String toString() {
            return "RpcConfig[idExtractionPath=" + Arrays.toString(idExtractionPath) + ", vModelId=" + vModelId + "]";
        }
    }

    private static final ObjectMapper mapper = new ObjectMapper()
            .setSerializationInclusion(Include.NON_NULL)
            .setSerializationInclusion(Include.NON_DEFAULT)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    private static final JavaType apiConfigType = mapper.getTypeFactory().constructType(ApiConfig.class);

    private volatile ApiConfig currentConfig;

    private DataplaneApiConfig() {}

    public static DataplaneApiConfig get() throws Exception {
        String envVarJson = getStringParameter(DATAPLANE_CFG_ENV_VAR, null);
        String jsonFile = getStringParameter(DATAPLANE_CFG_FILE_ENV_VAR, null);
        DataplaneApiConfig dac = new DataplaneApiConfig();
        if (envVarJson != null) {
            if (jsonFile != null) {
                throw new Exception("Setting both " + DATAPLANE_CFG_ENV_VAR
                                    + " and " + DATAPLANE_CFG_FILE_ENV_VAR + " env vars is not supported");
            }
            try {
                dac.updateConfig(mapper.readValue(envVarJson, apiConfigType));
            } catch (JsonProcessingException jpe) {
                throw new IOException("Error parsing dataplane api config json"
                                      + " from " + DATAPLANE_CFG_ENV_VAR + " env var", jpe);
            }
        } else if (jsonFile != null) {
            File file = new File(jsonFile);
            if (!file.exists()) {
                throw new Exception("File " + jsonFile + " configured via "
                                    + DATAPLANE_CFG_FILE_ENV_VAR + " env var does not exist");
            }
            ConfigMapKeyFileWatcher.startWatcher(file, dac::updateConfig,
                    DataplaneApiConfig::parseDacJson);
        } else {
            dac.updateConfig(new ApiConfig(Collections.emptyMap(), true));
        }
        return dac;
    }

    private static ApiConfig parseDacJson(Path path) throws IOException {
        try {
            logger.info("Loading dataplane api configuration from " + path);
            return mapper.readValue(path.toFile(), apiConfigType);
        } catch (JsonProcessingException jpe) {
            throw new IOException("Error parsing dataplane api config json from file " + path, jpe);
        }
    }

    /**
     * @return the current configuration
     */
    public ApiConfig getConfig() {
        return currentConfig;
    }

    void updateConfig(ApiConfig config) {
        currentConfig = config;
        logger.info("Dataplane API config set to: " + config);
    }

}
