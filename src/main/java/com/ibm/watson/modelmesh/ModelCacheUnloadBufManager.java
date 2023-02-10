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

import com.ibm.watson.modelmesh.ModelMesh.CacheEntry;
import com.ibm.watson.modelmesh.clhm.ConcurrentLinkedHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.GuardedBy;

import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.function.BooleanSupplier;

import static com.ibm.watson.modelmesh.ModelLoader.UNIT_SIZE;
import static com.ibm.watson.modelmesh.ModelMesh.mb;
import static java.lang.System.nanoTime;

/**
 * Cache bookeeping/synchronization related to model evictions and unloading.
 */
final class ModelCacheUnloadBufManager {
    private static final Logger logger = LogManager.getLogger(ModelCacheUnloadBufManager.class);

    private static final String UNLOAD_BUFFER_CACHE_KEY = "___UNLOADBUF";

    private final ConcurrentLinkedHashMap<String, CacheEntry<?>> runtimeCache;

    // The "permanent" cache entry used as a placeholder for
    // mem taken by in-progress model unloadings (which are
    // otherwise removed from the cache).
    // null => explicit unloading isn't enabled
    private final CacheEntry<?> UNLOAD_BUFF;

    // We share cache's internal eviction lock which guards any mutations that
    // affect total sizing as well as eviction callbacks
    private final Lock cacheLock;
    private final Condition cacheLockCondition;

    // Amount of space in the cache to reserve for unloads
    private final int unloadsReservedSizeUnits;
    // This is incremented/decremented based on unloading activity,
    // guarded by cacheLock
    private int totalUnloadingWeight;

    // The amount of space occupied by the unload buffer cache entry
    // at a given time is max(unloadsReservedSizeUnits, totalUnloadingWeight).
    // Note that totalUnloadingWeight can go negative since unloading
    // budget is also "borrowed" temporarily during model loading
    // to avoid cascading evictions

    // Total weighted cache size of loaded/loading models excluding the unload buffer.
    // It basically tracks (runtimeCache.weightedSize() - UNLOAD_BUFF.getWeight()),
    // but is accessed only under cacheLock in concert with totalUnloadingWeight updates.
    private long totalModelCacheOccupancy;

    // This tracks when we have overshot allocated cache size, which can happen if
    // "actual size" of model determined post-loading is much larger than the
    // predicted size or (unusual) when inserting the 1-unit placeholder for a new
    // model. Guarded by cacheLock.
    private int cacheDeficit;


    ModelCacheUnloadBufManager(ModelMesh mm, ConcurrentLinkedHashMap<String, CacheEntry<?>> cache,
        int unloadsReservedSizeUnits) {

        this.runtimeCache = cache;
        this.unloadsReservedSizeUnits = unloadsReservedSizeUnits;
        this.UNLOAD_BUFF = mm.newInternalCacheEntry(UNLOAD_BUFFER_CACHE_KEY, unloadsReservedSizeUnits);
        this.cacheLock = cache.getEvictionLock();
        this.cacheLockCondition = cacheLock.newCondition();
    }

    long getAdjustedCacheCapacity() {
        return runtimeCache.capacity() - UNLOAD_BUFF.getWeight();
    }

    int getUnloadBufferWeight() {
        return UNLOAD_BUFF.getWeight();
    }

    // For instrumentation/diagnostics only
    int getTotalUnloadingWeight() {
        return totalUnloadingWeight;
    }
    // For instrumentation/diagnostics only
    long getTotalModelCacheOccupancy() {
        return totalModelCacheOccupancy;
    }

    void removeUnloadBufferEntry(Map<String, ?> entries) { //TODO TBD maybe static
        entries.remove(UNLOAD_BUFFER_CACHE_KEY);
    }

    // ---------------- Pre-loading phase ----------------------

    /**
     * Safe insert which avoids cascading eviction: The minimal space required by the
     * placeholder entry (1 unit) is first "borrowed" from the unload buffer entry until
     * any unloads corresponding to resulting evictions have completed.
     * <p/>
     * This space "request" may subsequently be:
     * <ul>
     *   <li>Reverted via {@link #entryRemoved(int)}</li>
     *   <li>Increased via {@link #adjustNewEntrySpaceRequest(int, CacheEntry, boolean)}</li>
     *   <li>Waited-for and "claimed" via {@link #waitForSpaceToLoad(int, BooleanSupplier, long)}
     *       and {@link #claimRequestedSpaceIfReady(int)}</li>
     * </ul>
     *
     * @param modelId
     * @param ce
     * @param lastUsedTime
     */
    CacheEntry<?> insertNewEntry(String modelId, CacheEntry<?> ce, long lastUsedTime) {
        final int weight = ce.getWeight(); // weight here should be INSERTION_WEIGHT == 1
        cacheLock.lock();
        try {
            adjustAggregateUnloadingWeight(-weight);
            CacheEntry<?> existCe = runtimeCache.putIfAbsent(modelId, ce, lastUsedTime);
            if (existCe == null) {
                adjustTotalModelCacheOccupancy(weight); // insert succeeded
            } else {
                adjustAggregateUnloadingWeight(weight); // failed - pay back right away
            }
            return existCe;
        } finally {
            cacheLock.unlock();
        }
    }

    /**
     * @param increase must be > 0
     * @param entry
     * @param weakPrediction
     */
    void adjustNewEntrySpaceRequest(int increase, CacheEntry<?> entry, boolean weakPrediction) {
        final int newWeight = entry.getWeight() + increase;
        cacheLock.lock();
        try {
            // Reduce unload buffer allocation temporarily to prevent
            // cascading evictions. We block-wait until there is
            // cache space to increase this again (as unloads complete)
            // prior to the load actually starting
            adjustTotalModelCacheOccupancy(increase);
            adjustAggregateUnloadingWeight(-increase);
            entry.updateWeightLocked(weakPrediction ? -(newWeight) : newWeight);
        } finally {
            cacheLock.unlock();
        }
    }

    /**
     * This is the only blocking method in this class. Wait for previously "requested"
     * space to be fully available, i.e. once unloads resulting from triggered evictions
     * have completed.
     *
     * @param required
     * @param condition
     * @param deadlineNanos
     * @return true if space is ready or condition is met, false otherwise
     * @throws InterruptedException
     */
    boolean waitForSpaceToLoad(int required, BooleanSupplier condition, long deadlineNanos)
        throws InterruptedException {

        cacheLock.lock();
        try {
            while (!condition.getAsBoolean() && !cacheSpaceIsReady(required)) {
                long toWaitNanos = deadlineNanos - nanoTime();
                if (toWaitNanos <= 0) {
                    return false;
                }
                cacheLockCondition.awaitNanos(toWaitNanos); // throws InterruptedException
            }
        } finally {
            cacheLock.unlock();
        }
        return true;
    }

    /**
     * Non-blocking
     *
     * @param required
     * @return true if claim was successful, false otherwise
     */
    boolean claimRequestedSpaceIfReady(int required) {
        cacheLock.lock();
        try {
            if (cacheSpaceIsReady(required)) {
                adjustAggregateUnloadingWeight(required);
                return true;
            }
        } finally {
          cacheLock.unlock();
        }
        return false;
    }

    // -------------- Post-loading phase -----------------

    /**
     * Uses {@link #cacheDeficit} to grow already-loaded model entry, avoiding cascading eviction.
     *
     * @param delta
     * @param entry
     */
    void adjustWeightAfterLoad(final int delta, CacheEntry<?> entry) {
        if (delta == 0) return;
        cacheLock.lock();
        try {
            if (delta > 0) {
                final int deficit = delta - cacheRemaining();
                if (deficit > 0) {
                    adjustAggregateUnloadingWeight(-deficit);
                    cacheDeficit += deficit;
                    logger.warn("Memory over-allocation due to under-prediction of model size."
                        + " Cache deficit increased by " + deficit + " units to " + cacheDeficit
                        + " (" + mb(cacheDeficit * UNIT_SIZE) + ")");
                }
            }
            adjustTotalModelCacheOccupancy(delta);
            entry.updateWeightLocked(entry.getWeight() + delta);
            if (delta < 0) {
                payDownDeficitAndNotifyWaiters(-delta, false, true);
            }
        } finally {
          cacheLock.unlock();
        }
    }

    // Safe insert which avoids cascading eviction, makes use of cacheDeficit field if needed
    CacheEntry<?> insertFailedPlaceholderEntry(String modelId, CacheEntry<?> ce, long lastUsedTime) {
        final int weight = ce.getWeight(); // weight here should be INSERTION_WEIGHT == 1
        cacheLock.lock();
        try {
            final int deficit = weight - cacheRemaining();
            if (deficit > 0) {
                adjustAggregateUnloadingWeight(-deficit);
            }
            CacheEntry<?> existCe = runtimeCache.putIfAbsent(modelId, ce, lastUsedTime);
            if (existCe == null) {
                // insert succeeded
                adjustTotalModelCacheOccupancy(weight);
                if (deficit > 0) {
                    cacheDeficit += deficit;
                    logger.warn("Memory over-allocation due to cache placeholder entry insertion."
                            + " Cache deficit increased by " + deficit + " units to " + cacheDeficit
                            + " (" + mb(cacheDeficit * UNIT_SIZE) + ")");
                }
            } else if (deficit > 0) {
                adjustAggregateUnloadingWeight(deficit); // pay back right away
            }
            return existCe;
        } finally {
            cacheLock.unlock();
        }
    }

    // ---------- Unloading phase ------------------

    /**
     * @return the entry's weight at time of removal or -1 if the cache did not contain the entry
     */
    int removeEntry(CacheEntry<?> entry) {
        String modelId = entry.modelId;
        if (runtimeCache.getQuietly(modelId) != entry) {
            return -1;
        }
        cacheLock.lock();
        try {
            if (!runtimeCache.remove(modelId, entry)) {
                return -1;
            }
            int weight = entry.getWeight();
            entryRemoved(weight);
            return weight;
        } finally {
            cacheLock.unlock();
        }
    }

    /**
     * This is called "atomically" with a corresponding removal of an entry from the cache.
     * Either by the {@link #removeEntry(CacheEntry)} method above, or in the eviction callback.
     *
     * @param weight
     */
    @GuardedBy("cacheLock")
    void entryRemoved(int weight) {
        assert weight > 0;
        adjustTotalModelCacheOccupancy(-weight);
        adjustAggregateUnloadingWeight(weight);
    }

    void unloadComplete(int weight, boolean success, String modelId) {
        assert weight > 0;
        long capacity, newCapacity;
        cacheLock.lock();
        try {
            if (success) {
                payDownDeficitAndNotifyWaiters(weight, true, true);
                return;
            }
            // Exceptional case - loss of capacity due to failed unload (after retries)
            capacity = runtimeCache.capacity();
            newCapacity = Math.max(1L, capacity - weight);
            adjustAggregateUnloadingWeight(-weight);
            runtimeCache.setCapacity(newCapacity);
        } finally {
            cacheLock.unlock();
        }
        logger.warn("Failed unload of model " + modelId + " resulted in permanent capacity reduction of "
                + weight + " units (" + mb(weight * UNIT_SIZE) + ") from " + capacity + " to "
                + newCapacity);
    }

    void discardFailedEntry(int weight) {
        cacheLock.lock();
        try {
            adjustTotalModelCacheOccupancy(-weight);
            payDownDeficitAndNotifyWaiters(weight, false, true);
        } finally {
            cacheLock.unlock();
        }
    }


    // ------------- Private methods below here ----------

    private int cacheRemaining() {
        return (int) Math.min(runtimeCache.capacity() - runtimeCache.weightedSize(), Integer.MAX_VALUE);
    }

    @GuardedBy("cacheLock")
    private void payDownDeficitAndNotifyWaiters(int weight, boolean releaseFromUnloadingWeight, boolean notify) {
        assert weight > 0;
        int reduction = Math.min(weight, cacheDeficit);
        if (reduction != 0) {
            cacheDeficit -= reduction;
            weight -= reduction;
            if (cacheDeficit == 0) {
                logger.info("Cache deficit now fully repaid and memory is no longer over-allocated");
            }
        }
        adjustAggregateUnloadingWeight(releaseFromUnloadingWeight ? -weight : reduction);
        // If there is net freed space after paying any deficit, notifiy threads that may be waiting for that space.
        if (notify && weight > 0) {
            cacheLockCondition.signalAll();
        }
    }

    @GuardedBy("cacheLock")
    private void adjustTotalModelCacheOccupancy(int delta) {
        totalModelCacheOccupancy += delta;
    }

    @GuardedBy("cacheLock")
    private void adjustAggregateUnloadingWeight(int delta) {
        if (delta == 0) return;
        int newWeight = totalUnloadingWeight += delta;
        //TODO if new weight is bigger than X% of cache size,
        //  go into preservation mode [block new loads], + probably
        //  do controlled self-termination
        if (newWeight <= unloadsReservedSizeUnits) {
            newWeight = unloadsReservedSizeUnits;
        } else {
            int cap = (int) Math.min(runtimeCache.capacity(), Integer.MAX_VALUE);
            if (cap < newWeight) {
                newWeight = cap; // pathological case
                logger.warn("Entire cache capacity of " + cap + " units (" + mb(cap * UNIT_SIZE)
                    + ") is now taken up by removed models that are still unloading");
            }
        }
        UNLOAD_BUFF.updateWeightLocked(newWeight);
    }

    @GuardedBy("cacheLock")
    private boolean cacheSpaceIsReady(int required) {
        int newTuw = totalUnloadingWeight + required;
        if (newTuw <= unloadsReservedSizeUnits) {
            return true;
        }
        long totalRequired = newTuw + totalModelCacheOccupancy;
        return totalRequired <= runtimeCache.capacity();
    }
}
