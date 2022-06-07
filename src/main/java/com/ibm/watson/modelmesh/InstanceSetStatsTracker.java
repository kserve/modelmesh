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

import com.ibm.watson.modelmesh.ModelMesh.ClusterStats;
import com.ibm.watson.modelmesh.TypeConstraintManager.ProhibitedTypeSet;

import java.util.function.LongPredicate;

/**
 * Tracks aggregate statistics for subsets of instances as those instances come and go.
 * <p>
 * There is one of these per disjoint PTS "prohibited type set" (the set of instances
 * which share a common set of constraint-prohibited model types), plus one for the
 * the model-mesh cluster as a whole (all instances).
 */
final class InstanceSetStatsTracker {

    static final ClusterStats EMPTY_STATS = new ClusterStats(0L, 0L, Long.MAX_VALUE, 0, 0);

    private final LongPredicate isFull;
    final ProhibitedTypeSet prohibitedTypesSet;

    private long totalCapacity = 0, totalFree = 0, lru = Long.MAX_VALUE;
    private int count = 0, modelCount = 0;

    //TODO getter maybe
    volatile ClusterStats currentStats = EMPTY_STATS;

    public InstanceSetStatsTracker(ProhibitedTypeSet prohibitedTypesSet, LongPredicate isFull) {
        this.prohibitedTypesSet = prohibitedTypesSet;
        this.isFull = isFull;
    }

    int getInstanceCount() {
        return count;
    }

    void resetLru() {
        lru = Long.MAX_VALUE;
    }

    void addLru(long lru) {
        if (lru > 0L && lru < this.lru) {
            this.lru = lru;
        }
    }

    void add(String iid, InstanceRecord ir) {
        count++;
        modelCount += ir.getCount();
        totalCapacity += ir.getCapacity();
        long available = ir.getRemaining();
        // include in free total if not "full"
        if (!isFull.test(available)) {
            totalFree += available;
        }
    }

    boolean remove(String iid, InstanceRecord ir) {
        count--;
        modelCount -= ir.getCount();
        totalCapacity -= ir.getCapacity();
        long available = ir.getRemaining();
        // include in free total if not "full"
        if (!isFull.test(available)) {
            totalFree -= available;
        }
        return count <= 0;
    }

    ClusterStats update() {
        ClusterStats newStats = new ClusterStats(totalCapacity, totalFree, lru, count, modelCount);
        if (prohibitedTypesSet != null) {
            currentStats = newStats;
        }
        return newStats;
    }
}
