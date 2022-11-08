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

/**
 * Configuration environment variable constants
 */
public final class ModelMeshEnvVars {

    private ModelMeshEnvVars() {}

    // This must not be changed after model-mesh is already deployed to a particular env
    public static final String KV_STORE_PREFIX = "MM_KVSTORE_PREFIX";

    public static final String GRPC_PORT_ENV_VAR = "MM_SVC_GRPC_PORT";

    /** @deprecated */
    public static final String GRPC_PORT_ENV_VAR_OLD = "TAS_SVC_GRPC_PORT";

    public static final String GRPC_MAX_MSG_SIZE_ENV_VAR = "MM_SVC_GRPC_MAX_MSG_SIZE";
    public static final String GRPC_MAX_HEADERS_SIZE_ENV_VAR = "MM_SVC_GRPC_MAX_HEADERS_SIZE";

    public static final String LOCATION_ENV_VAR = "MM_LOCATION"; // set to kube node name
    public static final String ZONE_ENV_VAR = "MM_ZONE";
    public static final String LABELS_ENV_VAR = "MM_LABELS";

    public static final String SCALEUP_RPM_ENV_VAR = "MM_SCALEUP_RPM_THRESHOLD";

    public static final String MAX_INFLIGHT_PER_COPY_ENV_VAR = "MM_MAX_INFLIGHT_PER_MODEL_COPY";
    public static final String CONC_SCALEUP_BANDWIDTH_PCT_ENV_VAR = "MM_CONC_SCALEUP_BANDWIDTH_PCT";

    public static final String LOAD_FAILURE_EXPIRY_ENV_VAR = "MM_LOAD_FAILURE_EXPIRY_TIME_MS";

    public static final String MMESH_METRICS_ENV_VAR = "MM_METRICS";

    public static final String LOG_EACH_INVOKE_ENV_VAR = "MM_LOG_EACH_INVOKE";
    public static final String SEND_DEST_ID_ENV_VAR = "MM_SEND_DEST_ID";

    public static final String MM_MULTI_PARALLELISM_ENV_VAR = "MM_MULTI_PARALLELISM";

    public static final String KV_READ_ONLY_ENV_VAR = "KV_READ_ONLY";

    public static final String BOOTSTRAP_CLEARANCE_PERIOD_ENV_VAR = "BOOTSTRAP_CLEARANCE_PERIOD_MS";
    public static final String FAILFAST_UPGRADE_ENV_VAR = "MM_FAILFAST_UPGRADE_ENABLED";

    public static final String GRPC_MAX_CONNECTION_AGE_SECS_ENV_VAR = "MM_SVC_GRPC_MAX_CONNECTION_AGE_SECS";
    public static final String GRPC_MAX_CONNECTION_AGE_GRACE_SECS_ENV_VAR = "MM_SVC_GRPC_MAX_CONNECTION_AGE_GRACE_SECS";

    public static final String DEFAULT_VMODEL_OWNER = "MM_DEFAULT_VMODEL_OWNER";

    public static final String LOG_REQ_HEADER_CONFIG_ENV_VAR = "MM_LOG_REQUEST_HEADERS";

    // ---------------------------- Type Constraints --------------------------------

    public static final String TYPE_CONSTRAINTS_ENV_VAR = "MM_TYPE_CONSTRAINTS";
    public static final String TYPE_CONSTRAINTS_FILE_ENV_VAR = "MM_TYPE_CONSTRAINTS_PATH";

    // ---------------------------- DataPlane Config --------------------------------

    public static final String DATAPLANE_CFG_ENV_VAR = "MM_DATAPLANE_CONFIG";
    public static final String DATAPLANE_CFG_FILE_ENV_VAR = "MM_DATAPLANE_CONFIG_PATH";

    // ----------------------- Static Model Registration ----------------------------

    // There are two versions of each env var because of a prior inconsistency
    // (one used MODEL and the other MODELS)
    public static final String STATIC_REGISTRATIONS_ENV_VAR = "MM_AUTO_REGISTER_MODELS";
    public static final String STATIC_REGISTRATIONS_ENV_VAR2 = "MM_AUTO_REGISTER_MODEL";
    public static final String STATIC_REGISTRATIONS_FILE_ENV_VAR = "MM_AUTO_REGISTER_MODEL_PATH";
    public static final String STATIC_REGISTRATIONS_FILE_ENV_VAR2 = "MM_AUTO_REGISTER_MODELS_PATH";


    // ---------------------------- TLS Related --------------------------------------

    public static final String TLS_PRIV_KEY_PATH_ENV_VAR = "MM_TLS_PRIVATE_KEY_PATH";
    public static final String TLS_PRIV_KEY_PASSPHRASE_ENV_VAR = "MM_TLS_PRIVATE_KEY_PASSPHRASE";
    public static final String TLS_KEY_CERT_PATH_ENV_VAR = "MM_TLS_KEY_CERT_PATH";

    // values: NONE, REQUIRE, OPTIONAL
    public static final String TLS_CLIENT_AUTH_ENV_VAR = "MM_TLS_CLIENT_AUTH";
    // Can be single path or comma-separated list
    public static final String TLS_TRUST_CERT_PATH_ENV_VAR = "MM_TLS_TRUST_CERT_PATH";


    /**
     * @deprecated This env var name is misleading. It is currently used to provide
     * the certificates corresponding to the private key, not certs for client trust purposes
     * as its name implies.
     * <br />
     * Please use the equivalent "MM_TLS_KEY_CERT_PATH" instead.
     */
    public static final String TLS_KEY_CERT_PATH_ENV_VAR_OLD = "MM_SVC_GRPC_CA_CERT_PATH";
    /** @deprecated */
    public static final String TLS_KEY_CERT_PATH_ENV_VAR_OLD2 = "TAS_SVC_GRPC_CA_CERT_PATH";
    /**
     * @deprecated Please use "MM_TLS_PRIVATE_KEY_PATH" instead
     */
    public static final String TLS_PRIV_KEY_PATH_ENV_VAR_OLD = "MM_SVC_GRPC_PRIVATE_KEY_PATH";
    /** @deprecated */
    public static final String TLS_PRIV_KEY_PATH_ENV_VAR_OLD2 = "TAS_SVC_GRPC_PRIVATE_KEY_PATH";
    /** @deprecated */
    public static final String TLS_PRIV_KEY_PASSPHRASE_ENV_VAR_OLD = "MM_SVC_GRPC_PRIVATE_KEY_PASSPHRASE";
}
