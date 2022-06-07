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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapLikeType;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.ibm.watson.modelmesh.ModelMesh.ClusterStats;
import io.grpc.SynchronizationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;
import org.eclipse.collections.api.tuple.primitive.ObjectIntPair;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ibm.watson.modelmesh.ModelMesh.getStringParameter;
import static com.ibm.watson.modelmesh.ModelMeshEnvVars.TYPE_CONSTRAINTS_ENV_VAR;
import static com.ibm.watson.modelmesh.ModelMeshEnvVars.TYPE_CONSTRAINTS_FILE_ENV_VAR;
import static com.ibm.watson.modelmesh.Utils.empty;

/**
 * This class contains the logic for managing model type constraints, for heterogeneous
 * model-mesh clusters comprising more than one Deployment.
 * <p>
 * TODO(nick) more detailed documentation will be added to this class soon
 */
public final class TypeConstraintManager {
    private static final Logger logger = LogManager.getLogger(TypeConstraintManager.class);

    private static final String DEFAULT_TYPE_MAPPING = "_default";

    private static final ObjectMapper mapper = new ObjectMapper()
            .setSerializationInclusion(Include.NON_NULL)
            .setSerializationInclusion(Include.NON_DEFAULT)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final String[] NO_LABELS = new String[0];

    /**
     * The json config is deserialized into this class
     */
    static final class ConfigTypeConstraints {
        // these are kept in a normalized form: sorted, deduplicated and disjoint
        public final String[] required;
        public final String[] preferred;

        boolean isEmpty() {
            return empty(required) && empty(preferred);
        }

        public ConfigTypeConstraints() {
            this(NO_LABELS, NO_LABELS);
        }

        public ConfigTypeConstraints(String[] required, String[] preferred) {
            this.required = sortAndDeduplicate(required, null);
            this.preferred = sortAndDeduplicate(preferred, required);
        }

        @Override
        public String toString() {
            return "required=" + Arrays.toString(required)
                   + ", preferred=" + Arrays.toString(preferred);
        }
    }

    // Assumes exclude array is already sorted
    static String[] sortAndDeduplicate(String[] arr, String[] exclude) {
        if (empty(arr)) {
            return arr;
        }
        Arrays.sort(arr);
        int i = 0;
        for (String l : arr) {
            if ((i == 0 || !arr[i - 1].equals(l))
                && (exclude == null || Arrays.binarySearch(exclude, l) < 0)) {
                arr[i++] = l;
            }
        }
        return i == 0 ? NO_LABELS : i != arr.length ? Arrays.copyOf(arr, i) : arr;
    }

    private static final MapLikeType modelDefListType = mapper.getTypeFactory()
            .constructMapLikeType(Map.class, String.class, ConfigTypeConstraints.class); //TODO or HashMap

    // Reference to the "live" clusterState from the main ModelMesh class
    private final Set<Entry<String, InstanceRecord>> clusterState;
    // Predicate used to determine whether a particular instance is considered full
    // given its number of available units.
    private final LongPredicate isFull;

    private final String localInstanceId;

    // These two maps are accessed only from synchronization context

    // Maps lists of supported labels to instance sets (many-to-one)
    private final Map<String[], InstanceSetStatsTracker> labelsToInstanceSetStats =
            new TreeMap<>(Utils.STRING_ARRAY_COMP);
    // Maps "Prohibited Type Sets" to instance sets (one-to-one)
    private final Map<ProhibitedTypeSet, InstanceSetStatsTracker> ptsToInstanceSetStats = new HashMap<>();


    // This is accessed racily but changes only when the type mappings change
    private InstanceSetStatsTracker localInstanceSetStats;

    // This executor is used to serialize any changes to the local instance registry and/or
    // type constraint mappings
    private final Executor instanceChangeExecutor = new SynchronizationContext((t, e) -> {
        logger.error("Exception while processing instance or type-constraint changes", e);
    });

    // these are both copy-on-write
    private volatile Map<String, ModelTypeConstraints> typeConstraintsMap = Collections.emptyMap();
    // preferred instances to use for types with no constraints configured
    private volatile Set<String> defaultPreferredInstances; // null means "none"

    /**
     * Get TypeConstraintManager instance based on environment variable configuration
     */
    static TypeConstraintManager get(String localInstId, Set<Entry<String, InstanceRecord>> clusterState,
            LongPredicate isFull) throws Exception {
        String envVarJson = getStringParameter(TYPE_CONSTRAINTS_ENV_VAR, null);
        String jsonFile = getStringParameter(TYPE_CONSTRAINTS_FILE_ENV_VAR, null);
        if (envVarJson != null) {
            if (jsonFile != null) {
                throw new Exception("Setting both " + TYPE_CONSTRAINTS_ENV_VAR
                                    + " and " + TYPE_CONSTRAINTS_FILE_ENV_VAR + " env vars is not supported");
            }
            Map<String, ConfigTypeConstraints> config;
            try {
                config = mapper.readValue(envVarJson, modelDefListType);
            } catch (JsonProcessingException jpe) {
                throw new IOException("Error parsing type constraints json"
                                      + " from " + TYPE_CONSTRAINTS_ENV_VAR + " env var", jpe);
            }
            TypeConstraintManager tcm = new TypeConstraintManager(localInstId, clusterState, isFull);
            FutureTask<Void> future = new FutureTask<>(() -> updateTypeMappings(tcm, config), null);
            tcm.instanceChangeExecutor.execute(future);
            future.get(5, TimeUnit.SECONDS);
            return tcm;
        }
        if (jsonFile != null) {
            File file = new File(jsonFile);
            if (!file.exists()) {
                throw new Exception("File " + jsonFile + " configured via "
                                    + TYPE_CONSTRAINTS_FILE_ENV_VAR + " env var does not exist");
            }

            TypeConstraintManager tcm = new TypeConstraintManager(localInstId, clusterState, isFull);
            ConfigMapKeyFileWatcher.startWatcher(file,
                    cfg -> tcm.instanceChangeExecutor.execute(() -> updateTypeMappings(tcm, cfg)),
                    TypeConstraintManager::parseTcJson);
            return tcm;
        }
        return null;
    }

    //TODO move to instance method
    private static void updateTypeMappings(TypeConstraintManager tcm,
            Map<String, ConfigTypeConstraints> config) {
        logger.info("Model type placement constraint configuration:");
        int width = config.keySet().stream().mapToInt(String::length).max().orElse(0);
        for (Entry<String, ConfigTypeConstraints> ent : config.entrySet()) {
            logger.info(String.format(" - %-" + width + "s: %s", ent.getKey(), ent.getValue()));
        }
        tcm.typeMappingsUpdated(config);
        logger.info("Constraints configured for " + tcm.typeConstraintsMap.size() +
                    " types: " + tcm.typeConstraintsMap.keySet());
    }

    private static Map<String, ConfigTypeConstraints> parseTcJson(Path path) throws IOException {
        try {
            logger.info("Loading type constraints configuration from " + path);
            return mapper.readValue(path.toFile(), modelDefListType);
        } catch (JsonProcessingException jpe) {
            throw new IOException("Error parsing type constraints json from file " + path, jpe);
        }
    }

    private TypeConstraintManager(String localInstanceId,
            Set<Entry<String, InstanceRecord>> clusterState,
            LongPredicate isFull) {
        this.localInstanceId = localInstanceId;
        this.clusterState = clusterState;
        this.isFull = isFull;
    }

    Executor executor() {
        return instanceChangeExecutor;
    }

    /** @return null means use global cluster stats */
    ClusterStats getTypeSetStats(String type) {
        ModelTypeConstraints mtc = typeConstraintsMap.get(type);
        return mtc != null ? mtc.candidateSubsetStats() : null;
    }

    // Get stats for current instance (ourselves)
    ClusterStats getLocalInstanceSetStats() {
        InstanceSetStatsTracker liss = localInstanceSetStats;
        return liss != null ? liss.currentStats : InstanceSetStatsTracker.EMPTY_STATS;
    }

    /** @return null means "all" (no restrictions) */
    Set<String> getCandidateInstances(String type) {
        ModelTypeConstraints mtc = getTypeConstraints(type);
        return mtc != null ? mtc.allowedInstances : null;
    }

    /** @return null if no preferred instances */
    Set<String> getPreferredInstances(String type) {
        ModelTypeConstraints mtc = getTypeConstraints(type);
        return mtc != null ? mtc.preferredInstances : defaultPreferredInstances;
    }

    String[] getRequiredLabels(String type) {
        ModelTypeConstraints mtc = getTypeConstraints(type);
        return mtc != null ? mtc.requiredLabels : null;
    }

    private ModelTypeConstraints getTypeConstraints(String type) {
        final Map<String, ModelTypeConstraints> tcm = typeConstraintsMap;
        ModelTypeConstraints mtc = tcm.get(type);
        return mtc != null ? mtc : tcm.get(DEFAULT_TYPE_MAPPING);
    }

    private static final Comparator<InstanceSetStatsTracker> PARTITION_STATS_COMP = (isst1, isst2) -> {
        final ClusterStats cs1 = isst1.currentStats, cs2 = isst2.currentStats;
        int diff = Long.compare(cs2.totalFree, cs1.totalFree);
        if (diff != 0) return diff;
        diff = Long.compare(cs1.globalLru, cs2.globalLru);
        if (diff != 0) return diff;
        return Long.compare(cs2.totalCapacity, cs1.totalCapacity);
    };

    /**
     * Called from the reaper thread
     *
     * @return a list of current {@link InstanceSetStatsTracker} objects, one for each
     * partition of instances corresponding to a unique prohibited type set.
     */
    List<InstanceSetStatsTracker> getPartitionStats() {
        // This is not called from the instance-updating executor and so must be explicity run
        // on that since the ptsToInstanceSetStats map is not threadsafe.
        final SettableFuture<List<InstanceSetStatsTracker>> future = SettableFuture.create();
        executor().execute(() -> {
            List<InstanceSetStatsTracker> result = new ArrayList<>(ptsToInstanceSetStats.values());
            result.sort(PARTITION_STATS_COMP);
            future.set(result);
        });
        return Futures.getUnchecked(future);
    }

    /**
     * ProhibitedTypeSet is the set of types associated with one or more instances,
     * where models of those types cannot be loaded on those instances. This class is immutable.
     */
    static final class ProhibitedTypeSet implements Comparable<ProhibitedTypeSet> {
        private final String [] types; // sorted
        private final int hashCode;

        public ProhibitedTypeSet(Collection<String> types) {
            this.types = types.toArray(NO_LABELS);
            Arrays.sort(this.types);
            this.hashCode = Stream.of(this.types).mapToInt(String::hashCode).sum();
        }

        public int size() {
            return types.length;
        }

        public boolean contains(String type) {
            return Arrays.binarySearch(types, type) >= 0;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ProhibitedTypeSet
                    && Arrays.equals(types, ((ProhibitedTypeSet) obj).types);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public String toString() {
            return "PTS-" + Integer.toHexString(hashCode) + Arrays.toString(types);
        }

        @Override
        public int compareTo(ProhibitedTypeSet o) {
            return Utils.STRING_ARRAY_COMP.compare(types, o.types);
        }
    }

    // Tracks per-modeltype requirements and preferences, and the resulting current sets
    // of instances on which models of the type are allowed/preferred to be placed
    static final class ModelTypeConstraints {
        // both assumed to be sorted
        private final String[] requiredLabels; // never null
        private final String[] preferredLabels; // never null

        // null when no required instances (iff requiredLabels.length == 0)
        // empty when no instances match requirements
        final Set<String> allowedInstances;
        // null when no preferred instances. May be populated with "inferred"
        // preferred instances based on preferences of other types when no
        // explicit preferences are configured (i.e. to prefer instances which
        // other types _don't_ prefer)
        final Set<String> preferredInstances;
        // null when no preferred instances configured
        final Set<String> configuredPreferredInstances;

        // null means "all instances" (i.e. use global clusterStats)
        final InstanceSetStatsTracker[] instanceSetStats;

        ClusterStats candidateSubsetStats() {
            if (instanceSetStats == null) {
                return null;
            }
            if (instanceSetStats.length == 1) {
                return instanceSetStats[0].currentStats;
            }
            long capacity = 0, free = 0, lru = Long.MAX_VALUE;
            int count = 0, modelCopyCount = 0;
            for (InstanceSetStatsTracker instanceSet : instanceSetStats) {
                ClusterStats subStats = instanceSet.currentStats;
                capacity += subStats.totalCapacity;
                free += subStats.totalFree;
                count += subStats.instanceCount;
                modelCopyCount += subStats.modelCopyCount;
                if (subStats.globalLru < lru) {
                    lru = subStats.globalLru;
                }
            }
            return new ClusterStats(capacity, free, lru, count, modelCopyCount);
        }

        ModelTypeConstraints(String[] requiredLabels, String[] preferredLabels,
                Set<String> allowedInstances, Set<String> configuredPreferredInstances,
                InstanceSetStatsTracker[] instanceSetStats,
                Set<String> resolvedPreferredInstances) {
            this.requiredLabels = requiredLabels;
            this.preferredLabels = preferredLabels;
            this.allowedInstances = allowedInstances;
            this.configuredPreferredInstances = configuredPreferredInstances;
            this.preferredInstances = resolvedPreferredInstances;
            this.instanceSetStats = instanceSetStats;

        }

        /**
         * @return {@link ModelTypeConstraints} with instanceSetStats set to provided value
         * (either this or a copy)
         */
        ModelTypeConstraints updateInstanceSetStats(Set<InstanceSetStatsTracker> newStats,
                Set<String> newInferredPreferred) {
            boolean inferredMatch = Objects.equal(preferredInstances, newInferredPreferred);
            InstanceSetStatsTracker[] newStatArray;
            if (newStats == null) {
                if (instanceSetStats == null && inferredMatch) {
                    return this;
                }
                newStatArray = null;
            } else {
                boolean statsMatch = instanceSetStats != null && newStats.size() == instanceSetStats.length
                                     && Arrays.stream(instanceSetStats).allMatch(newStats::contains);
                if (statsMatch && inferredMatch) {
                    return this;
                }
                newStatArray = newStats.toArray(InstanceSetStatsTracker[]::new);
            }
            return new ModelTypeConstraints(requiredLabels, preferredLabels,
                    allowedInstances, configuredPreferredInstances, newStatArray, newInferredPreferred);
        }

        static ModelTypeConstraints fromInstanceSet(String[] requiredLabels, String[] preferredLabels,
                Set<Map.Entry<String, InstanceRecord>> instances, String typeName,
                InstanceSetStatsTracker[] instanceSetStats) {
            // assumption is that requiredLabels and preferredLabels are already sorted
            ImmutableSet.Builder<String> required = requiredLabels.length == 0
                    ? null : ImmutableSet.builder();
            ImmutableSet.Builder<String> preferred = null;
            for (Map.Entry<String, InstanceRecord> ent : instances) {
                String iid = ent.getKey();
                String[] instanceLabels = ent.getValue().getLabels();
                if (required != null && instanceMatches(instanceLabels, requiredLabels, true)) {
                    required.add(iid);
                } else if (instanceMatches(instanceLabels, preferredLabels, false)) {
                    if (preferred == null) {
                        preferred = ImmutableSet.builder();
                    }
                    preferred.add(iid);
                }
            }
            // required == null means "no requirements for this type"
            // required is empty set means "no instances satisfy the requirements"
            Set<String> requiredInstances = null;
            if (required != null) {
                requiredInstances = required.build();
                if (requiredInstances.isEmpty()) {
                    logger.warn("There are currently no instances that satisfy the label"
                                + " requirements for type " + typeName + ": " + Arrays.toString(requiredLabels));
                }
            }
            Set<String> preferredSet = preferred != null ? preferred.build() : null;
            return new ModelTypeConstraints(requiredLabels, preferredLabels,
                    requiredInstances, preferredSet, instanceSetStats, preferredSet);
        }

        boolean allowedOnInstance(String iid) {
            return allowedInstances == null || allowedInstances.contains(iid);
        }

        /**
         * assumes input arrays are sorted
         */
        boolean labelsMatch(String[] required, String[] preferred) {
            return Arrays.equals(requiredLabels, required)
                   && Arrays.equals(preferredLabels, preferred);
        }

        /** @return new ModelTypeConstraints if changed, {@code this} otherwise */
        ModelTypeConstraints updateInstance(String iid, String[] labels) {
            Set<String> newReqInstances = allowedInstances;
            if (newReqInstances != null) {
                // if type has no requirements then nothing will change here
                newReqInstances = updateInstanceSet(iid, labels,
                        requiredLabels, allowedInstances, true);
            }
            Set<String> newPrefInstances = updateInstanceSet(iid, labels,
                    preferredLabels, configuredPreferredInstances, false);
            return newReqInstances == allowedInstances && newPrefInstances == configuredPreferredInstances
                    ? this : new ModelTypeConstraints(requiredLabels, preferredLabels,
                    newReqInstances, newPrefInstances, instanceSetStats, newPrefInstances);
        }

        // instanceLabels must be sorted
        private static boolean instanceMatches(String[] instanceLabels, String[] typeLabels,
                boolean matchAll) {
            if (instanceLabels.length == 0 || typeLabels.length == 0) {
                return false;
            }
            Predicate<String> hasLabel = l -> Arrays.binarySearch(instanceLabels, l) >= 0;
            Stream<String> labelStream = Arrays.stream(typeLabels);
            return matchAll ? labelStream.allMatch(hasLabel) : labelStream.anyMatch(hasLabel);
        }

        /** @return new set if changed, passed-in set otherwise (could be null in either case) */
        private static Set<String> updateInstanceSet(String iid, String[] instanceLabels,
                String[] typeLabels, Set<String> instanceSet, boolean matchAll) {
            boolean curMatch = instanceSet != null && instanceSet.contains(iid);
            if (instanceMatches(instanceLabels, typeLabels, matchAll)) {
                if (!curMatch) {
                    return instanceSet != null ? ImmutableSet.<String>builderWithExpectedSize(instanceSet.size() + 1)
                            .addAll(instanceSet).add(iid).build() : Collections.singleton(iid);
                }
            } else if (curMatch) {
                if (instanceSet.size() == 1) {
                    return matchAll ? Collections.emptySet() : null;
                }
                return ImmutableSet.<String>builder()
                        .addAll(Sets.filter(instanceSet, e -> !e.equals(iid))).build();
            }
            return instanceSet;
        }
    }

    InstanceSetStatsTracker getStatsForLabels(String[] labels) {
        return labelsToInstanceSetStats.get(labels);
    }

    /** Called from within synchronization context (executor) */
    InstanceSetStatsTracker instanceAdded(String iid, String[] labels, boolean includedInStats) {
        assert labels != null;
        final Map<String, ModelTypeConstraints> mtcMap = typeConstraintsMap;
        final Map<String, ModelTypeConstraints> newMap = instanceUpdated(iid, labels, mtcMap);
        InstanceSetStatsTracker instanceSetStats = getInstanceSetStats(iid, labels, newMap);
        if (newMap != mtcMap) {
            typeConstraintsMap = ImmutableMap.copyOf(refreshPerTypeInstanceSets(newMap));
        }
        int count = instanceSetStats.getInstanceCount() + (includedInStats ? 0 : 1);
        logger.info("Added instance " + iid + " to " + instanceSetStats.prohibitedTypesSet
                    + ", now includes " + count + " instance" + (count == 1 ? "" : "s"));
        return instanceSetStats;
    }

    void instanceRemoved(String iid, String[] labels) {
        final Map<String, ModelTypeConstraints> mtcMap = typeConstraintsMap;
        final Map<String, ModelTypeConstraints> newMap = instanceUpdated(iid, NO_LABELS, mtcMap);
        final InstanceSetStatsTracker instanceSetStats = labelsToInstanceSetStats.get(labels);
        if (instanceSetStats != null) {
            ProhibitedTypeSet pts = instanceSetStats.prohibitedTypesSet;
            if (instanceSetStats.getInstanceCount() == 0) {
                // none left, remove them
                labelsToInstanceSetStats.remove(labels);
                ptsToInstanceSetStats.remove(pts);
                if (newMap != mtcMap) {
                    refreshPerTypeInstanceSets(newMap);
                }
            }
            int count = instanceSetStats.getInstanceCount();
            logger.info("Removed instance " + iid + " from "
                        + pts + ", now includes " + count
                        + " instance" + (count == 1 ? "" : "s"));
        }
        if (newMap != mtcMap) {
            typeConstraintsMap = ImmutableMap.copyOf(newMap);
        }
    }

    /**
     * Called from within synchronization context (executor)
     *
     * @return the current instance-set that the specified instance belongs to,
     * creating it if necessary
     */
    private InstanceSetStatsTracker getInstanceSetStats(String iid, String[] labels,
            Map<String, ModelTypeConstraints> tcMap) {
        InstanceSetStatsTracker instanceSetStats = labelsToInstanceSetStats.get(labels);
        if (instanceSetStats == null) {
            ArrayList<String> newPts = new ArrayList<>(tcMap.size());
            for (Entry<String, ModelTypeConstraints> ent : tcMap.entrySet()) {
                if (!ent.getValue().allowedOnInstance(iid)) {
                    newPts.add(ent.getKey());
                }
            }
            ProhibitedTypeSet pts = new ProhibitedTypeSet(newPts);
            instanceSetStats = ptsToInstanceSetStats.get(pts);
            if (instanceSetStats == null) {
                instanceSetStats = new InstanceSetStatsTracker(pts, isFull);
                ptsToInstanceSetStats.put(pts, instanceSetStats);
                labelsToInstanceSetStats.put(labels, instanceSetStats);
                if (iid.equals(localInstanceId)) {
                    localInstanceSetStats = instanceSetStats;
                }
            }
        }
        return instanceSetStats;
    }

    private static Map<String, ModelTypeConstraints> instanceUpdated(String iid, String[] labels,
            Map<String, ModelTypeConstraints> mtcMap) {
        HashMap<String, ModelTypeConstraints> newMap = null;
        for (Entry<String, ModelTypeConstraints> ent : mtcMap.entrySet()) {
            ModelTypeConstraints mtc = ent.getValue(), newMtc = mtc.updateInstance(iid, labels);
            if (newMtc != mtc) {
                if (newMap == null) {
                    newMap = new HashMap<>(mtcMap);
                }
                newMap.put(ent.getKey(), newMtc);
                if (mtc.requiredLabels.length != 0 && newMtc.allowedInstances.isEmpty()
                    && !mtc.allowedInstances.isEmpty()) {
                    logger.warn("There are currently no running instances which match the"
                                + " label requirements of type " + ent.getKey()
                                + ": " + Arrays.toString(mtc.requiredLabels));
                }
            }
        }
        return newMap != null ? newMap : mtcMap;
    }

    /**
     * Called from within synchronization context (executor)
     *
     * @param newConfig assumed to be mutable, and ownership transferred (this method will modify)
     */
    void typeMappingsUpdated(Map<String, ConfigTypeConstraints> newConfig) {
        Map<String, ModelTypeConstraints> mtcMap = typeConstraintsMap, newMap = null;

        // Handle existing types
        for (Entry<String, ModelTypeConstraints> ent : mtcMap.entrySet()) {
            String typeName = ent.getKey();
            ConfigTypeConstraints tc = newConfig.remove(typeName);
            if (tc == null || tc.isEmpty()) {
                if (tc != null) {
                    logger.warn("Empty configured constraints for type "
                                + typeName + " (no labels)");
                }
                if (newMap == null) {
                    newMap = new HashMap<>(mtcMap);
                }
                newMap.remove(typeName);
            } else {
                ModelTypeConstraints mtc = ent.getValue();
                if (!mtc.labelsMatch(tc.required, tc.preferred)) {
                    // labels changed, lets recreate
                    if (newMap == null) {
                        newMap = new HashMap<>(mtcMap);
                    }
                    newMap.put(typeName, ModelTypeConstraints.fromInstanceSet(
                            tc.required, tc.preferred, clusterState, typeName,
                            mtc.instanceSetStats));
                }
            }
        }
        // Handle any *new* types (existing were removed from deserialized map above)
        for (Entry<String, ConfigTypeConstraints> ent : newConfig.entrySet()) {
            ConfigTypeConstraints tc = ent.getValue();
            if (tc.isEmpty()) {
                logger.warn("Ignoring empty configured constraints for type "
                            + ent.getKey() + " (no labels)");
            }
            if (newMap == null) {
                newMap = new HashMap<>(mtcMap);
            }
            newMap.put(ent.getKey(), ModelTypeConstraints.fromInstanceSet(
                    tc.required, tc.preferred, clusterState, ent.getKey(), null));
        }
        if (newMap != null) {
            if (!ptsToInstanceSetStats.isEmpty()) {
                logger.info("Clearing " + ptsToInstanceSetStats.size() + " prohibited type sets before repopulation: "
                        + ptsToInstanceSetStats.entrySet().stream()
                        .map(e -> e.getKey() + ": " + e.getValue().getInstanceCount())
                        .collect(Collectors.joining(", ", "{", "}")));
            }
            labelsToInstanceSetStats.clear();
            ptsToInstanceSetStats.clear();
            for (Map.Entry<String, InstanceRecord> ent : clusterState) {
                String iid = ent.getKey();
                InstanceRecord ir = ent.getValue();
                InstanceSetStatsTracker isst = getInstanceSetStats(iid, ir.getLabels(), newMap);
                isst.add(iid, ir);
                ir.prohibitedTypes = isst.prohibitedTypesSet;
            }
            ptsToInstanceSetStats.values().forEach(InstanceSetStatsTracker::update);
            typeConstraintsMap = ImmutableMap.copyOf(refreshPerTypeInstanceSets(newMap));
        }
    }

    /**
     * This updates the instanceSetStats and preferredInstances fields of all of
     * the per-type ModelTypeConstraints as appropriate, following instance addition/removal or
     * updated type constraint configuration.
     * <p>
     * preferredInstances are inferred when none are configured, based on the constraints/preferences
     * of other types (prefer to go on instances on which other types aren't allowed or don't prefer)
     *
     * @param mtcMap assumed to be mutable
     */
    private Map<String, ModelTypeConstraints> refreshPerTypeInstanceSets(Map<String, ModelTypeConstraints> mtcMap) {

        // Calculate a desirability score for each instance based on which types permit and prefer
        // placement there. Start with the count of prohibited types * 4 and subtract count of preferred types
        MutableObjectIntMap<String> instanceScores = new ObjectIntHashMap<String>(clusterState.size());
        for (Map.Entry<String, InstanceRecord> ent : clusterState) {
            instanceScores.put(ent.getKey(), ent.getValue().prohibitedTypes.size() * 4);
        }
        for (Map.Entry<String, ModelTypeConstraints> ent : mtcMap.entrySet()) {
            Set<String> preferred = ent.getValue().configuredPreferredInstances;
            if (preferred != null) {
                for (String p : preferred) {
                    if (instanceScores.containsKey(p)) {
                        instanceScores.addToValue(p, -1);
                    }
                }
            }
        }

        Set<String> defaultPreferred = inferPreferredInstances(instanceScores, null);
        if (!Objects.equal(defaultPreferredInstances, defaultPreferred)) {
            defaultPreferredInstances = defaultPreferred;
            logger.info("Default preferred instances updated to: "
                        + (defaultPreferred != null ? defaultPreferred : "<none>"));
        }

        Set<InstanceSetStatsTracker> statSet = new HashSet<>(ptsToInstanceSetStats.size());
        for (Map.Entry<String, ModelTypeConstraints> ent : mtcMap.entrySet()) {
            ModelTypeConstraints mtc = ent.getValue();
            if (mtc.allowedInstances == null) {
                ent.setValue(mtc.updateInstanceSetStats(null, defaultPreferred));
            } else {
                statSet.clear();
                for (Entry<ProhibitedTypeSet, InstanceSetStatsTracker> pts : ptsToInstanceSetStats.entrySet()) {
                    if (!pts.getKey().contains(ent.getKey())) {
                        statSet.add(pts.getValue());
                    }
                }
                Set<String> inferredPreferred = mtc.configuredPreferredInstances != null
                                                || mtc.allowedInstances.isEmpty() ? mtc.preferredInstances
                        : inferPreferredInstances(instanceScores, mtc.allowedInstances);
                ent.setValue(mtc.updateInstanceSetStats(statSet, inferredPreferred));
            }
        }
        return mtcMap;
    }

    private static Set<String> inferPreferredInstances(ObjectIntMap<String> instanceScores, Set<String> include) {
        Set<String> instanceIds = new HashSet<>(include != null ? include.size() : 8);
        int min = Integer.MAX_VALUE, max = 0;
        for (ObjectIntPair<String> ent : instanceScores.keyValuesView()) {
            if (include != null && !include.contains(ent.getOne())) {
                continue;
            }
            int score = ent.getTwo();
            if (score < min) {
                min = score;
            }
            if (score >= max) {
                if (score > max) {
                    instanceIds.clear();
                    max = score;
                }
                instanceIds.add(ent.getOne());
            }
        }
        return min < max ? ImmutableSet.copyOf(instanceIds) : null;
    }
}
