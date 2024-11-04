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

import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.api.map.primitive.ObjectLongMap;
import org.eclipse.collections.impl.factory.primitive.ObjectLongMaps;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class contains logic to track patterns of instances (pods) coming and going,
 * in particular taking into account the type constraint labels and replicaset
 * of the pod in question (latter inferred based on instance id convention of
 * last 12 chars of pod name).
 * <p>
 * Based on this it keeps a set of replicaset names which are very likely to be in
 * the process of scaling down to zero as part of a rolling Deployment update. These
 * are then used by the placement logic to exclude pods belonging to those
 * replicasets from consideration, unless there are no other suitable options.
 * <p>
 * Note that this is complicated somewhat by the fact that there could be multiple
 * Deployments making up a single cluster when type constraints are in play. But
 * it's assumed that there will only be one Deployment for a given set of labels.
 */
public final class UpgradeTracker {

    static final long TEN_MINS = 600_000L;
    static final long FIFTEEN_MINS = 900_000L;
    static final long TWENTY_MINS = 1200_000L;

    static final class ReplicaSetStats {
        int size;
        long earliestStartTime = Long.MAX_VALUE;
        long latestStartTime;
        long lastChangeTime;
    }

    @SuppressWarnings("serial")
    static final class PerTypeLabelStats extends HashMap<String, ReplicaSetStats> {
        public PerTypeLabelStats() {
            super(2);
        }
    }

    // This is accessed only by instance updater thread
    private final Map<List<String>, PerTypeLabelStats> upgradeTracker = new HashMap<>(1);

    // Map from replicaset to expiry time, updated via copy-on-write by instance updater thread.
    // Copy-on-write since updates will be be rare and we want to optimize for reads.
    private volatile ObjectLongMap<String> likelyReplacedReplicaSets = ObjectLongMaps.immutable.empty();

    /**
     * Called from arbitrary (request) threads.
     *
     * @return map whose keys are replicasets that should be avoided for model placements
     */
    public ObjectLongMap<String> getLikelyReplacedReplicaSets() {
        return likelyReplacedReplicaSets;
    }

    /**
     * Called only from instance-updating thread/context
     */
    void instanceRemoved(String iid, InstanceRecord ir) {
        if (iid.length() < 7) {
            return; // instance ids must be of non-standard format
        }
        PerTypeLabelStats ptls = upgradeTracker.get(Arrays.asList(ir.getLabels()));
        if (ptls == null) {
            return;
        }
        // First 6 chars of instance id comes from replicaset name
        String replicaSet = iid.substring(0, 6);
        ReplicaSetStats rss = ptls.get(replicaSet);
        if (rss != null) {
            long now = System.currentTimeMillis();
            rss.size--;
            if (rss.size > 0) {
                rss.lastChangeTime = now;
            } else {
                // last one, clean it up
                ptls.remove(replicaSet);
            }
            if (likelyReplacedReplicaSets.containsKey(replicaSet)) {
                MutableObjectLongMap<String> replacement = new ObjectLongHashMap<>(likelyReplacedReplicaSets);
                if (rss.size <= 0) {
                    replacement.remove(replicaSet);
                } else {
                    replacement.put(replicaSet, rss.lastChangeTime + FIFTEEN_MINS);
                }
                likelyReplacedReplicaSets = replacement;
            }
        }
    }

    /**
     * Called only from instance-updating thread/context
     */
    void instanceAdded(String iid, InstanceRecord ir) {
        if (iid.length() < 7) {
            return; // instance ids must be of non-standard format
        }
        PerTypeLabelStats ptls = upgradeTracker.get(Arrays.asList(ir.getLabels()));
        if (ptls == null) {
            upgradeTracker.put(Arrays.asList(ir.getLabels()), ptls = new PerTypeLabelStats());
        }
        long now = System.currentTimeMillis();
        // First 6 chars of instance id comes from replicaset name
        String replicaSetId = iid.substring(0, 6);
        ReplicaSetStats rss = ptls.get(replicaSetId);
        if (rss == null) {
            ptls.put(replicaSetId, rss = new ReplicaSetStats());
        }
        rss.lastChangeTime = now;
        rss.size++;
        long startTime = ir.getStartTime();
        if (startTime < rss.earliestStartTime) {
            rss.earliestStartTime = startTime;
        }
        if (startTime > rss.latestStartTime) {
            rss.latestStartTime = startTime;
        }

        Set<String> old = Collections.emptySet();
        if (ptls.size() > 1) {
            // Find replicaset with same labels which was started most recently
            ReplicaSetStats newest = ptls.values().stream().max((rs1, rs2)
                    -> Long.compare(rs1.earliestStartTime, rs2.earliestStartTime)).get();
            // If within 20 minutes, find any replicasets which have not had any
            // instances started since the newest one was started, but have had _some_
            // change within the last 15 mins (instance removals). These ones are very
            // likely in the process of being replaced in a rolling update.
            if (newest.latestStartTime > now - TWENTY_MINS) {
                old = ptls.entrySet().stream()
                        .filter(e -> e.getValue().latestStartTime < newest.earliestStartTime &&
                                     (newest.latestStartTime > now - TEN_MINS
                                      || e.getValue().lastChangeTime > now - FIFTEEN_MINS))
                        .map(Entry::getKey).collect(Collectors.toSet());
            }
        }

        if (likelyReplacedReplicaSets.isEmpty() && old.isEmpty()) {
            return;
        }
        MutableObjectLongMap<String> replacement = null;
        for (Map.Entry<String, ReplicaSetStats> ent : ptls.entrySet()) {
            replicaSetId = ent.getKey();
            if (old.contains(replicaSetId)) {
                if (!likelyReplacedReplicaSets.containsKey(replicaSetId)) {
                    if (replacement == null) {
                        replacement = new ObjectLongHashMap<>(likelyReplacedReplicaSets);
                    }
                    // Add entry or update with new expiry time
                    replacement.put(replicaSetId, ent.getValue().lastChangeTime + FIFTEEN_MINS);
                }
            } else if (likelyReplacedReplicaSets.containsKey(replicaSetId)) {
                if (replacement == null) {
                    replacement = new ObjectLongHashMap<>(likelyReplacedReplicaSets);
                }
                replacement.remove(replicaSetId);
            }
        }
        if (replacement != null) {
            likelyReplacedReplicaSets = replacement; // atomically update volatile
        }
    }

    /**
     * Called only from instance-updating thread/context
     */
    void doHousekeeping() {
        // Check for and remove expired entries from the likelyReplacedReplicaSets map
        ObjectLongMap<String> replacedReplicatSets = likelyReplacedReplicaSets;
        if (!replacedReplicatSets.isEmpty()) {
            long now = System.currentTimeMillis();
            if (replacedReplicatSets.anySatisfy(expires -> now >= expires)) {
                likelyReplacedReplicaSets = replacedReplicatSets.reject((r, expires) -> now >= expires);
            }
        }
    }
}
