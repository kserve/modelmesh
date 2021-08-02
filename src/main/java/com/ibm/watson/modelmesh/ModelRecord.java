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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.ibm.watson.kvutils.JsonExtensible;
import com.ibm.watson.kvutils.KVRecord;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 */
public final class ModelRecord extends KVRecord {

    // Constant map used to avoid allocation in default/common no-failure case
    private static final Map<String, FailureInfo> EMPTY = Collections.emptyMap();

    static final class FailureInfo extends JsonExtensible {
        @JsonProperty("msg")
        private final String message; // never null

        @JsonCreator
        public FailureInfo(@JsonProperty("msg") String message) {
            this.message = Strings.nullToEmpty(message);
        }

        public String getMessage() {
            return message;
        }

        protected FailureInfo() {
            this("");
        }

        @Override
        public String toString() {
            return message;
        }
    }

    @JsonProperty("type")
    private final String type;
    @JsonProperty("encKey")
    private final String encryptionKey;
    @JsonProperty("mPath")
    private final String modelPath;

    // Map of instanceId -> (start of) loaded time
    private final Map<String, Long> instanceIds = new TreeMap<>();

    // Map of instanceId -> loading failed time
    @JsonProperty("failedIn")
    private final Map<String, Long> loadFailedInstanceIds = new TreeMap<>();

    // Map of instanceId -> load failure info, secondary to loadFailedInstanceIds
    // field. Separate map for backward-compatibility.
    @JsonProperty("fails")
    private Map<String, FailureInfo> failures = EMPTY;

    /**
     * Atomically tracks dependent records. These might include other models
     * which require this (this is a "base" model), and/or vmodels to which
     * this model belongs. Deletion is blocked when nonzero.
     */
    @JsonProperty("refs")
    private int refCount;

    /**
     * If true, this model will be automatically deleted if it's refCount
     * reaches zero
     */
    @JsonProperty("autoDel")
    private final boolean autoDelete;

    /**
     * Last-used time of this model. Note this intentionally updated in a lazy
     * fashion, specifically only when the record is being updated for some other
     * reason, *or* the "real" lastUsed value is more than 4 hours later (as
     * tracked by instances' in-mem caches).
     * <p>
     * It's currently only used to prioritize which models should be loaded
     * proactively when there is available space in the cluster.
     */
    @JsonProperty("lu")
    private long lastUsed;

    /**
     * Last time that a model was unloaded. Not guaranteed to be set and won't
     * be set if instanceIds.size() <= 2.
     * <p>
     * Currently only used in model scale-down logic.
     */
    @JsonProperty("lul")
    private long lastUnloadTime;

    @JsonCreator
    public ModelRecord(@JsonProperty("type") String type,
            @JsonProperty("encKey") String encryptionKey,
            @JsonProperty("mPath") String modelPath,
            @JsonProperty("autoDel") boolean autoDelete) {
        //TODO workaround, not sure why this should be necessary with jackson defaults handling
        this.type = type != null ? type : DEFAULT_TYPE;
        this.encryptionKey = encryptionKey;
        this.modelPath = modelPath;
        this.autoDelete = autoDelete;
    }

    // required for registry backwards compatibility
    //  - absence of type in serialized model record => "NLCLASSIFIER"
    public static final String DEFAULT_TYPE = "NLCLASSIFIER";

    protected ModelRecord() {
        this(DEFAULT_TYPE, null, null /*TBD*/, false); // for jackson defaults comparison only
    }

    public String getType() {
        return type;
    }

    public String getModelPath() {
        return modelPath;
    }

    public String getEncryptionKey() {
        return encryptionKey;
    }

    public Map<String, Long> getInstanceIds() {
        return instanceIds;
    }

    public Map<String, Long> getLoadFailedInstanceIds() {
        return loadFailedInstanceIds;
    }

    public void addLoadFailure(String instanceId, Long timestamp, String message) {
        loadFailedInstanceIds.put(instanceId, timestamp);
        if (!Strings.isNullOrEmpty(message)) {
            if (failures == EMPTY) {
                failures = new TreeMap<>();
            }
            failures.put(instanceId, new FailureInfo(message));
        } else {
            failures.remove(instanceId);
        }
        instanceIds.remove(instanceId);
    }

    public boolean removeLoadFailure(String instanceId) {
        return loadFailedInstanceIds.remove(instanceId) != null | failures.remove(instanceId) != null;
    }

    public boolean removeLoadFailure(String instanceId, Long timestamp) {
        if (loadFailedInstanceIds.remove(instanceId, timestamp)) {
            failures.remove(instanceId);
            return true;
        }
        return false;
    }

    public boolean hasLoadFailure() {
        return !loadFailedInstanceIds.isEmpty();
    }

    public boolean loadFailedInInstance(String instanceId) {
        return loadFailedInstanceIds.containsKey(instanceId);
    }

    /**
     * The returned map should not be modified directly
     */
    @JsonIgnore
    public Map<String, FailureInfo> getLoadFailureInfos() {
        return failures;
    }

    public String getLoadFailureMessage(String instanceId) {
        FailureInfo fi = failures.get(instanceId);
        return fi != null ? fi.message : null;
    }

    public int getRefCount() {
        return refCount;
    }

    public void setRefCount(int refCount) {
        this.refCount = refCount;
    }

    /**
     * @return new ref count
     */
    public int decRefCount() {
        return refCount > 0 ? --refCount : 0;
    }

    /**
     * @return new ref count
     */
    public int incRefCount() {
        return ++refCount;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public long getLastUsed() {
        return lastUsed;
    }

    public void setLastUsed(long lastUsed) {
        this.lastUsed = lastUsed;
    }

    /**
     * @param lastUsed 0 means "now"
     */
    public void updateLastUsed(long lastUsed) {
        if (lastUsed == 0L) {
            lastUsed = System.currentTimeMillis();
        }
        if (lastUsed > this.lastUsed) {
            this.lastUsed = lastUsed;
        }
    }

    /**
     * @return 0 means "not set", does *not* mean "now"
     */
    public long getLastUnloadTime() {
        return lastUnloadTime;
    }

    public void setLastUnloadTime(long lastUnloadTime) {
        this.lastUnloadTime = lastUnloadTime;
    }

    // called after unloaded model has been removed from instanceIds
    public void updateLastUnloadTime() {
        this.lastUnloadTime = instanceIds.size() <= 2 ? 0L : System.currentTimeMillis();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ModelRecord [type=").append(type).append(", encryptionKey=")
                .append(encryptionKey == null ? "null" : "[HIDDEN (non-null)]").append(", modelPath=").append(modelPath)
                .append(", loadFailedInstanceIds=").append(loadFailedInstanceIds).append(", instanceIds=")
                .append(instanceIds).append(", refs=").append(refCount).append(", autoDelete=").append(autoDelete)
                .append(", lastUsed=" + lastUsed);
        if (lastUsed > 0L) {
            sb.append(" (" + ModelMesh.readableTime(System.currentTimeMillis() - lastUsed) + " ago)");
        }
        sb.append(", lastUnload=" + lastUnloadTime);
        if (lastUnloadTime > 0L) {
            sb.append(" (" + ModelMesh.readableTime(System.currentTimeMillis() - lastUnloadTime) + " ago)");
        }
        return sb.append("]").toString();
    }

}
