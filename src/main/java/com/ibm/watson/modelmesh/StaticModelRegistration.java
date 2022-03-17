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

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionLikeType;
import com.google.common.base.Strings;
import com.ibm.watson.litelinks.ThreadContext;
import com.ibm.watson.modelmesh.StaticModelDefiniton.ModelInfo;
import com.ibm.watson.modelmesh.api.ModelStatusInfo.ModelStatus;
import com.ibm.watson.modelmesh.api.SetVModelRequest;
import com.ibm.watson.modelmesh.api.VModelStatusInfo;
import com.ibm.watson.modelmesh.thrift.Status;
import com.ibm.watson.modelmesh.thrift.StatusInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.ibm.watson.modelmesh.ModelMesh.getStringParameter;
import static com.ibm.watson.modelmesh.ModelMeshEnvVars.*;

/**
 *
 */
public final class StaticModelRegistration {
    private static final Logger logger = LoggerFactory.getLogger(StaticModelRegistration.class);

    private static final ObjectMapper mapper = new ObjectMapper()
            .setSerializationInclusion(Include.NON_NULL)
            .setSerializationInclusion(Include.NON_DEFAULT)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final CollectionLikeType modelDefListType = mapper.getTypeFactory()
            .constructCollectionLikeType(List.class, StaticModelDefiniton.class);

    public static void registerAndVerify(ModelMesh modelMesh, VModelManager vmm) throws Exception {

        String envVarJson = getStringParameter(STATIC_REGISTRATIONS_ENV_VAR, null);
        if (envVarJson == null) {
            envVarJson = getStringParameter(STATIC_REGISTRATIONS_ENV_VAR2, null);
        }

        String jsonFile = getStringParameter(STATIC_REGISTRATIONS_FILE_ENV_VAR, null);
        if (jsonFile == null) {
            jsonFile = getStringParameter(STATIC_REGISTRATIONS_FILE_ENV_VAR2, null);
        }

        if (envVarJson == null && jsonFile == null) {
            return;
        }

        List<StaticModelDefiniton> allDefs = new ArrayList<>();
        if (jsonFile != null) {
            allDefs.addAll(mapper.readValue(new File(jsonFile), modelDefListType));
            logger.info("Found models defined for static registration in file " + jsonFile);
        }
        if (envVarJson != null) {
            allDefs.addAll(mapper.readValue(envVarJson, modelDefListType));
            logger.info("Found models defined for static registration in env var " + STATIC_REGISTRATIONS_ENV_VAR);
        }
        if (allDefs.isEmpty()) {
            return;
        }

        if (modelMesh.readOnlyMode) {
            logger.warn("Skipping all static model/vmodel registration/verification"
                        + " because model-mesh is in read-only mode");
            return;
        }

        logger.info(allDefs.size() + " models configured for static registration:");
        for (StaticModelDefiniton def : allDefs) {
            logger.info(" - " + def);
        }

        HashSet<String> waitFor = new HashSet<>();

        for (StaticModelDefiniton def : allDefs) {
            String modelId = def.getModelId();
            if (Strings.isNullOrEmpty(def.getModelId())) {
                throw new Exception("Configured static model must have string modelId attribute");
            }
            ModelInfo modelInfo = def.getModelInfo();
            if (modelInfo == null || Strings.isNullOrEmpty(modelInfo.getType())) {
                throw new Exception("Must specify non-empty modelInfo.type for model " + modelId);
            }
            String vModelId = def.getvModelId();
            if (vModelId == null && def.isRetainAfterTransition()) {
                throw new Exception("retainAfterTransition attribute is only applicable"
                                    + " when specifying a vModelId, for model " + modelId);
            }

            boolean ensureLoaded = def.isEnsureLoaded();
            ThreadContext.removeCurrentContext(); // ensure context is clear
            if (ensureLoaded) {
                // add flag to ensure our instance isn't specially favoured for placement
                ThreadContext.addContextEntry(ModelMesh.UNBALANCED_KEY, "true");
            }
            if (vModelId == null) {
                logger.info("About to statically register/verify model " + modelId);
                com.ibm.watson.modelmesh.thrift.ModelInfo mi = new com.ibm.watson.modelmesh.thrift.ModelInfo();
                mi.setServiceType(modelInfo.getType());
                mi.setModelPath(modelInfo.getPath());
                mi.setEncKey(modelInfo.getKey());
                StatusInfo si = modelMesh.registerModel(modelId, mi, ensureLoaded, false, 0L); //TODO timestamp tbd
                if (ensureLoaded && si.getStatus() != Status.LOADED) {
                    waitFor.add(modelId);
                }
            } else {
                logger.info("About to statically register/verify VModel " + vModelId + " and target model " + modelId);
                com.ibm.watson.modelmesh.api.ModelInfo mi = com.ibm.watson.modelmesh.api.ModelInfo.newBuilder()
                        .setType(modelInfo.getType()).setPath(modelInfo.getPath()).setKey(modelInfo.getKey()).build();
                VModelStatusInfo vmsi = vmm.updateVModel(SetVModelRequest.newBuilder().setVModelId(vModelId)
                        .setAutoDeleteTargetModel(!def.isRetainAfterTransition()).setLoadNow(ensureLoaded)
                        .setTargetModelId(modelId).setModelInfo(mi).build());
                if (ensureLoaded && (!modelId.equals(vmsi.getActiveModelId())
                                     || vmsi.getActiveModelStatus().getStatus() != ModelStatus.LOADED)) {
                    waitFor.add(modelId);
                }
            }

            //TODO TBD about failure case failing startup
            // maybe special context parameter to force retry in this case i.e. expire eldest failure record

            //TODO check/handle case where we're waiting for it to load and the model is deleted (e.g. vmodel transition)
        }

        if (!waitFor.isEmpty()) {
            logger.info("Waiting for " + waitFor.size() + " models to be loaded before proceeding: " + waitFor);
            long oneYearAgo = System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(365L, TimeUnit.DAYS);
            for (String modelId : waitFor) {
                // Use very old timestamp which will effectively leave the existing one unchanged
                StatusInfo si = modelMesh.ensureLoadedInternal(modelId, oneYearAgo, 0, null, 0, true);
                if (si.getStatus() == Status.LOADING_FAILED) {
                    logger.error("Model " + modelId + " has status " + Status.LOADING_FAILED + ": "
                                 + si.getErrorMessages() + ", aborting");
                    throw new Exception(
                            "Loading of statically-registered model " + modelId + " failed: " + si.getErrorMessages());
                }
                logger.info("Model " + si + " has status " + si.getStatus());
            }
            logger.info("All statically registered models now verified");
        }
    }
}
