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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ibm.watson.kvutils.KVRecord;

import java.util.Objects;

public class VModelRecord extends KVRecord {

    @JsonProperty("o")
    private final String owner;

    // both activeModel and targetModel should never be null

    @JsonProperty("amid")
    private String activeModel;

    @JsonProperty("tmid")
    private String targetModel;

    /**
     * Can only be true when activeModel != targetModel
     */
    @JsonProperty("failed")
    private boolean targetLoadFailed;

    public VModelRecord(String initialModel, String owner) {
        this.owner = owner;
        this.targetModel = initialModel;
        this.activeModel = initialModel;
    }

    protected VModelRecord() { // for jackson defaults comparison only
        this(null, null);
    }

    public String getOwner() {
        return owner;
    }

    public String getActiveModel() {
        return activeModel;
    }

    public void setActiveModel(String activeModel) {
        this.activeModel = activeModel;
    }

    public String getTargetModel() {
        return targetModel;
    }

    public void setTargetModel(String targetModel) {
        this.targetModel = targetModel;
    }

    public boolean isTargetLoadFailed() {
        return targetLoadFailed;
    }

    public void setTargetLoadFailed(boolean targetLoadFailed) {
        this.targetLoadFailed = targetLoadFailed;
    }

    public boolean inTransition() {
        return !Objects.equals(activeModel, targetModel);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("VModelRecord [active=").append(activeModel);
        if (inTransition()) {
            sb.append(", target=").append(targetModel);
            if (targetLoadFailed) sb.append(" (LOAD FAILED)");
        }
        if (owner != null) sb.append(", owner=").append(owner);
        return sb.append(']').toString();
    }
}
