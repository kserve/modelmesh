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
import com.ibm.watson.kvutils.KVRecord;
import com.ibm.watson.modelmesh.TypeConstraintManager.ProhibitedTypeSet;

import java.util.Arrays;
import java.util.Objects;

import static com.ibm.watson.modelmesh.ModelMesh.readableTime;

/**
 *
 */
public class InstanceRecord extends KVRecord {

    private static final String[] NO_LABELS = new String[0];

    @JsonProperty("lruTime")
    private long lruTime; // Long.MAX_VALUE when empty
    @JsonProperty("count")
    private int count;
    @JsonProperty("cap")
    private long capacity; // weighted capacity estimate
    @JsonProperty("used")
    private long used; // weighted usage
    @JsonProperty("lThreads")
    private int loadingThreads; // # configured loading threads
    @JsonProperty("lInProg")
    private int loadingInProgress; // total # loads in progress
    // "busyness": rate of model invocations, or percentage
    // processing time (in 1/10th pct units) if in time-tracking mode
    @JsonProperty("rpm")
    private int reqsPerMinute;

    // when true, equivalent to the record being deleted;
    // currently exists only to help monitor operational state
    @JsonProperty("shutdown")
    private boolean shuttingDown;

    @JsonProperty("startTime")
    private final long startTime;
    @JsonProperty("vers")
    private final long instanceVersion;
    @JsonProperty("loc")
    private final String location;
    @JsonProperty("zone")
    private final String zone;

    @JsonProperty("labels")
    private final String[] labels; // non-null, always sorted

    // This is set only when type constraints are in use
    @JsonIgnore
    transient volatile ProhibitedTypeSet prohibitedTypes;

    InstanceRecord() { // just for jackson defaults resolution
        this(0, 0, null, null, NO_LABELS);
    }

    @JsonCreator
    public InstanceRecord(@JsonProperty("startTime") long startTime,
            @JsonProperty("vers") long instanceVersion,
            @JsonProperty("loc") String location, @JsonProperty("zone") String zone,
            @JsonProperty("labels") String[] labels) {
        this.startTime = startTime;
        this.instanceVersion = instanceVersion;
        this.location = location;
        this.zone = zone;
        if (labels == null || labels.length == 0) {
            this.labels = NO_LABELS;
        } else {
            this.labels = labels;
            Arrays.sort(labels);
        }
        this.actionableUpdate = true;
    }

    public InstanceRecord(long startTime, long instanceVersion,
            String location, String zone, String[] labels,
            long lruTime, int count, long capacity, long used, int loadingThreads,
            int loadingInProgress, boolean shuttingDown) {
        this(startTime, instanceVersion, location, zone, labels);
        this.lruTime = lruTime;
        this.count = count;
        this.capacity = capacity;
        this.used = used;
        this.loadingThreads = loadingThreads;
        this.loadingInProgress = loadingInProgress;
        this.shuttingDown = shuttingDown;
    }

    /** millisecs since epoch, Long.MAX_VALUE if empty */
    public long getLruTime() {
        return lruTime;
    }

    /** millisecs since epoch, Long.MAX_VALUE if empty */
    public void setLruTime(long lruTime) {
        this.lruTime = lruTime;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    /** In 8KiB units */
    public long getCapacity() {
        return capacity;
    }

    /** In 8KiB units */
    public void setCapacity(long capacity) {
        this.capacity = capacity;
    }

    /** In 8KiB units */
    public long getUsed() {
        return used;
    }

    /** In 8KiB units */
    public void setUsed(long used) {
        this.used = used;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getInstanceVersion() {
        return instanceVersion;
    }

    public String getLocation() {
        return location;
    }

    public String getZone() {
        return zone;
    }

    public String[] getLabels() {
        return labels;
    }

    public int getLoadingThreads() {
        return loadingThreads;
    }

    public void setLoadingThreads(int loadingThreads) {
        this.loadingThreads = loadingThreads;
    }

    public int getLoadingInProgress() {
        return loadingInProgress;
    }

    public void setLoadingInProgress(int loadingInProgress) {
        this.loadingInProgress = loadingInProgress;
    }

    // note in time-tracking mode this is actually a busyness measure
    public int getReqsPerMinute() {
        return reqsPerMinute;
    }

    public void setReqsPerMinute(int reqsPerMinute) {
        this.reqsPerMinute = reqsPerMinute;
    }

    public boolean isShuttingDown() {
        return shuttingDown;
    }

    public void setShuttingDown(boolean shuttingDown) {
        this.shuttingDown = shuttingDown;
    }

    @JsonIgnore
    public long getRemaining() {
        return Math.max(0L, capacity - used);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + (int) (capacity ^ (capacity >>> 32));
        result = prime * result + count;
        result = prime * result + loadingThreads;
        result = prime * result + loadingInProgress;
        result = prime * result + reqsPerMinute;
        result = prime * result + (location == null ? 0 : location.hashCode());
        result = prime * result + (zone == null ? 0 : zone.hashCode());
        result = prime * result + Arrays.hashCode(labels);
        result = prime * result + (int) (lruTime ^ (lruTime >>> 32));
        result = prime * result + (shuttingDown ? 1231 : 1237);
        return prime * result + (int) (used ^ (used >>> 32));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        InstanceRecord other = (InstanceRecord) obj;
        if (capacity != other.capacity) return false;
        if (count != other.count) return false;
        if (lruTime != other.lruTime) return false;
        if (used != other.used) return false;
        if (loadingThreads != other.loadingThreads) return false;
        if (loadingInProgress != other.loadingInProgress) return false;
        if (reqsPerMinute != other.reqsPerMinute) return false;
        if (shuttingDown != other.shuttingDown) return false;
        if (!Objects.equals(zone, other.zone)) return false;
        if (!Arrays.equals(labels, other.labels)) return false;
        return Objects.equals(location, other.location);
    }

    @Override
    public String toString() {
        long now = System.currentTimeMillis();
        int pct = capacity > 0 ? (int) (used * 100L / capacity) : 0;
        String lruStr = lruTime < 0L || lruTime == Long.MAX_VALUE ? "never"
                : (lruTime == 0 ? "0" : (lruTime + " (" + readableTime(now - lruTime) + " ago)"));
        return "InstanceRecord [lruTime=" + lruStr + ", count=" + count + ", capacity=" + capacity + ", used=" + used
               + " (" + pct + "%), loc=" + location + ", zone=" + (zone != null ? zone : "<none>")
               + ", labels=" + Arrays.toString(labels)
               + (startTime > 0L ? (", startTime=" + startTime + " (" + readableTime(now - startTime) + " ago)") : "")
               + ", vers=" + instanceVersion + ", loadThreads=" + loadingThreads + ", loadInProg=" + loadingInProgress
               + ", reqsPerMin=" + reqsPerMinute + (shuttingDown ? " SHUTTING-DOWN" : "") + "]";
    }

}
