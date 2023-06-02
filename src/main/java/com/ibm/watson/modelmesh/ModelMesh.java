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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.*;
import com.googlecode.concurrentlinkedhashmap.Weigher;
import com.ibm.etcd.client.FutureListener;
import com.ibm.watson.kvutils.DynamicConfig;
import com.ibm.watson.kvutils.JsonSerializer;
import com.ibm.watson.kvutils.KVTable;
import com.ibm.watson.kvutils.KVTable.EventType;
import com.ibm.watson.kvutils.LeaderElection;
import com.ibm.watson.kvutils.SessionNode;
import com.ibm.watson.kvutils.factory.KVUtilsFactory;
import com.ibm.watson.litelinks.LitelinksEnvVariableNames;
import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import com.ibm.watson.litelinks.ThreadContext;
import com.ibm.watson.litelinks.ThreadPoolHelper;
import com.ibm.watson.litelinks.client.LitelinksServiceClient;
import com.ibm.watson.litelinks.client.LitelinksServiceClient.ServiceInstanceInfo;
import com.ibm.watson.litelinks.client.LoadBalancer;
import com.ibm.watson.litelinks.client.ServiceInstance;
import com.ibm.watson.litelinks.client.ServiceUnavailableException;
import com.ibm.watson.litelinks.client.ThriftClientBuilder;
import com.ibm.watson.litelinks.server.DefaultThriftServer;
import com.ibm.watson.litelinks.server.Idempotent;
import com.ibm.watson.litelinks.server.ServiceDeploymentInfo;
import com.ibm.watson.litelinks.server.ThriftService;
import com.ibm.watson.modelmesh.ModelLoader.LoadedRuntime;
import com.ibm.watson.modelmesh.ModelMesh.ExtendedStatusInfo.CopyInfo;
import com.ibm.watson.modelmesh.ModelRecord.FailureInfo;
import com.ibm.watson.modelmesh.TypeConstraintManager.ProhibitedTypeSet;
import com.ibm.watson.modelmesh.clhm.ConcurrentLinkedHashMap;
import com.ibm.watson.modelmesh.clhm.ConcurrentLinkedHashMap.EvictionListenerWithTime;
import com.ibm.watson.modelmesh.payload.AsyncPayloadProcessor;
import com.ibm.watson.modelmesh.payload.CompositePayloadProcessor;
import com.ibm.watson.modelmesh.payload.LoggingPayloadProcessor;
import com.ibm.watson.modelmesh.payload.MatchingPayloadProcessor;
import com.ibm.watson.modelmesh.payload.PayloadProcessor;
import com.ibm.watson.modelmesh.payload.RemotePayloadProcessor;
import com.ibm.watson.modelmesh.thrift.ApplierException;
import com.ibm.watson.modelmesh.thrift.BaseModelMeshService;
import com.ibm.watson.modelmesh.thrift.InternalException;
import com.ibm.watson.modelmesh.thrift.InvalidInputException;
import com.ibm.watson.modelmesh.thrift.InvalidStateException;
import com.ibm.watson.modelmesh.thrift.ModelInfo;
import com.ibm.watson.modelmesh.thrift.ModelLoadException;
import com.ibm.watson.modelmesh.thrift.ModelNotFoundException;
import com.ibm.watson.modelmesh.thrift.ModelNotHereException;
import com.ibm.watson.modelmesh.thrift.Status;
import com.ibm.watson.modelmesh.thrift.StatusInfo;
import io.grpc.Context;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import org.apache.curator.utils.ZKPaths;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.map.primitive.ObjectLongMap;
import org.eclipse.collections.impl.factory.primitive.ObjectLongMaps;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

import javax.annotation.concurrent.GuardedBy;
import java.io.File;
import java.io.InterruptedIOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.channels.ClosedByInterruptException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.ibm.watson.kvutils.KVTable.EventType.*;
import static com.ibm.watson.modelmesh.ModelLoader.UNIT_SIZE;
import static com.ibm.watson.modelmesh.ModelMeshEnvVars.*;
import static com.ibm.watson.modelmesh.SidecarModelMesh.trimStack;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.*;

/**
 * Model-mesh primary service class
 */
public abstract class ModelMesh extends ThriftService
        implements BaseModelMeshService.Iface,
                   EvictionListenerWithTime<String, ModelMesh.CacheEntry<?>>, // AvailabilityListener,
                   KVTable.Listener<ModelRecord> {

    private static final Logger logger = LogManager.getLogger(ModelMesh.class);

    public static long BOOTSTRAP_CLEARANCE_PERIOD_MS = 3 * 60 * 1000L; // default value, may be overridden during init

    public static final int DEFAULT_GRPC_MAX_MSG_SIZE = 16 * 1024 * 1024; // 16MiB

    public final int GRPC_PORT; // set to 0 to disable

    {
        String grpcPort = getStringParameter(GRPC_PORT_ENV_VAR, null);
        if (grpcPort == null) {
            grpcPort = getStringParameter(GRPC_PORT_ENV_VAR_OLD, null);
            if (grpcPort != null) {
                logger.warn(GRPC_PORT_ENV_VAR_OLD + " env var is deprecated, please use "
                            + GRPC_PORT_ENV_VAR + " instead");
            }
        }
        try {
            GRPC_PORT = grpcPort == null ? 0 : Integer.parseInt(grpcPort);
        } catch (NumberFormatException nfe) {
            throw new NumberFormatException(GRPC_PORT_ENV_VAR + " is not a valid port number: " + grpcPort);
        }
    }

    private static final int REGISTRY_BUCKETS = 128; // this should NOT be changed

    @Deprecated
    public static final String SHARED_KV_CONFIG_PATH = ZKPaths.makePath("/shared", "config");

    // config parameters residing in kv store (some dynamic)
    public static final String LOGGER_LEVEL_PARAM = "logger_level";
    public static final String LOG_EACH_INVOCATION = "log_each_invocation";
    public static final String SEND_DEST_ID_PARAM = "send_destination_id";
    public static final String SCALEUP_RPM_PARAM = "scaleup_rpm_threshold";
    public static final String DISABLE_SLOT_PARAM = "disable";

    // litelinks registration metadata parameter
    public static final String KV_STORE_TYPE_META = "kv_store_type";

    private static final String KV_STORE_PREFIX; // must not change this value for a given environment

    static {
        String kvStorePrefix = System.getenv(ModelMeshEnvVars.KV_STORE_PREFIX);
        if (kvStorePrefix == null) {
            kvStorePrefix = "tas-runtime"; // default for backwards compatibility
        } else if (kvStorePrefix.matches(".*[\\s/].*")) {
            throw new RuntimeException(ModelMeshEnvVars.KV_STORE_PREFIX
                                       + " env var must not contain whitespace or / char");
        }
        KV_STORE_PREFIX = kvStorePrefix;
    }

    // just for debugging/development
    static final boolean DISABLE_PROACTIVE_LOADING = false;

    // used only during migration between disjoint kv stores (e.g. zookeeper -> etcd).
    // blocks addition/removal of models, and ensure proactive loads include models
    // where the only associated instances are not present in the "visible" instance
    // registry (they may be loaded in instances configured against the other kv store)
    protected final boolean readOnlyMode;

    // set to Metrics.NO_OP_METRICS if disabled
    protected /*final*/ Metrics metrics = Metrics.NO_OP_METRICS;

    // so that metrics field doesn't have to be volatile
    private volatile boolean metricsSetup;

    // timeout for model loads (doesn't include any time in queue)
    protected /*final*/ long loadTimeoutMs;

    // used in routing decisions, gets set to Math.max(3000L, loadTimeoutMs/3)
    protected /*final*/ long defaultAssumeLoadedAfterMs;

    // time after which loading failure records expire (allowing for re-attempts)
    public final long LOAD_FAILURE_EXPIRY_MS = getLongParameter(LOAD_FAILURE_EXPIRY_ENV_VAR, 900_000L); // default 15mins
    // shorter expiry time for "in use" models (receiving recent requests)
    public final long IN_USE_LOAD_FAILURE_EXPIRY_MS = LOAD_FAILURE_EXPIRY_MS / 2;
    public static final int MAX_LOAD_FAILURES = 3;
    // if unable to invoke in this many places, don't continue to load
    public static final int MAX_LOAD_LOCATIONS = 5;

    // the cache is prevented from "turning over" at a rate faster than this.
    // defaults to 10 * defaultAssumeLoadedAfterMs
    protected /*final*/ long minChurnAgeMs;

    // these values somewhat arbitrary, could be tuned
    public static final long INSTANCE_REC_PUBLISH_FREQ_MS = 40_000L;
    public static final long INSTANCE_REC_PUBLISH_MIN_PERIOD_MS = 2_000L;

    // how often the local janitor task runs (in every instance)
    public final long LOCAL_JANITOR_FREQ_SECS = Long.getLong("tas.janitor_freq_secs", 6 * 60); // 6mins

    // interval for rate checking task which tracks rate of reqs to each model
    protected final long RATE_CHECK_INTERVAL_MS = Long.getLong("tas.ratecheck_freq_ms", 10_000L); // 10sec

    protected static final int DEFAULT_SCALEUP_RPM = 2000;

    /* These are the "ages" between which a prior use of a single-copy model will
     * trigger a second copy to be loaded. Note that the actual precision corresponds
     * to the period of the rateTrackingTask which has a default of 10 seconds.
     * By default these values are 40 and 7 minutes respectively.
     * This is a heuristic for identifying "regular" usage as apposed to one-off
     * or short burst.
     */
    protected final int SECOND_COPY_MAX_AGE_SECS = Integer.getInteger("mm.max_second_copy_age_secs", 2400);
    protected final int SECOND_COPY_MIN_AGE_SECS = Integer.getInteger("mm.min_second_copy_age_secs", 420);

    /* Last-used age after which second model copies will be removed
     * (2->1 scaledown). Note this is a maximum and a smaller value might
     * be used if the global age of the cache is small (less than 10x this value).
     * Also note that scale-downs are only done if the cache is (globally) full.
     */
    public static final long SECOND_COPY_REMOVE_MAX_AGE_MS = 10 * 3600_000L; // 10hours

    /* When models are added to the cluster via the registerModel method with the
     * load parameter set to true, the recency timestamp of the loaded
     * model will be set to this amount of time in the past.
     * It is beneficial in cases when the cluster is overwhelmed -
     * ensuring newly added models won't push out ones that are known
     * to have been used within this period.
     */
    public static final long LASTUSED_AGE_ON_ADD_MS = 3600_000; // 1hour

    // how often the registry reaper task runs (only in leader)
    public static final long REGISTRY_REAPER_FREQ_MINS = 7;
    public static final long ASSUME_INSTANCE_GONE_AFTER_MS = 10 * 60_000L; // 10min

    static final long Mi = 1024L * 1024, Gi = Mi * 1024;
    static final long M = 1000_000L, G = M * 1000;

    // time before which we don't wait for migrated models to load elsewhere during pre-shutdown
    protected static final long CUTOFF_AGE_MS = 60 * 60_000L; // 1 hour

    // when expiring failure records, use the shorter age if recent requests for the model
    // have been seen within this time
    protected static final long SHORT_EXPIRY_RECENT_USE_TIME_MS = 3 * 60_000L; // 3mins

    // max combined number of cache-hit/miss retries per request - mainly just a safeguard
    protected static final int MAX_ITERATIONS = 8;

    // max number of RESOURCE_EXCEEDED failures per request
    protected static final int MAX_RES_EXHAUSTED = 2;

    // size of threadpool used for loading models
    protected /*final*/ int loadingThreads;

    protected static final int TASK_THREADS = 10;

    // conservative global default average model size - in 8KiB units
    private /*final*/ int defaultModelSizeUnits;

    // number of units of free space below which an instance is considered not full
    private /*final*/ int minSpaceUnits;

    // null means no restriction
    private /*final*/ Set<String> acceptedModelTypes;

    // optional - only used if external grpc port is set
    private /*final*/ ModelMeshApi grpcServer;

    protected final KVUtilsFactory kvUtilsFactory;

    private /*final*/ KVTable kvTable;
    private /*final*/ KVTable.TableView<ModelRecord> registry;

    private /*final*/ VModelManager vModelManager;

    private /*final*/ KVTable instanceTable;
    private /*final*/ KVTable.TableView<InstanceRecord> instanceInfo;
    private /*final*/ SessionNode myNode;

    private /*final*/ String instanceLocation; // e.g. node
    private /*final*/ String instanceZone;
    private /*final*/ String[] instanceLabels; // non-null

    private /*final*/ TypeConstraintManager typeConstraints; // null if not configured

    // leadership is just for which instance runs the
    // registry reaper task
    private /*final*/ LeaderElection leaderLatch;

    protected /*final*/ DynamicConfig config;

    /*
     * The contents of this is kept in sync with the instanceInfo table,
     * but ordered by most desirable location for placing new models
     */
    private /*final*/ SortedSet<Entry<String, InstanceRecord>> clusterState;
    // These stats are refreshed upon any changes to the _global_ clusterState
    private volatile ClusterStats clusterStats = new ClusterStats(0L, 0L, Long.MAX_VALUE, 0, 0);

    private final UpgradeTracker upgradeTracker = new UpgradeTracker();

    // This is used only by the leader - records time instances are first found missing,
    // so it knows when to consider them gone
    private final ConcurrentMap<String, Long> missings = new ConcurrentHashMap<>(8, .75f, 2);

    // These only used only by the leader and only for the purposes of metrics tracking/publishing
    private final Set<String> loadedModelIds = new HashSet<>(), failedModelIds = new HashSet<>();
    private volatile int loadedModelCount = -1, failedModelCount = -1;

    private final JsonSerializer<InstanceRecord> INST_REC_SERIALIZER = new JsonSerializer<>(InstanceRecord.class);

    private /*final*/ ConcurrentLinkedHashMap<String, CacheEntry<?>> runtimeCache;

    private /*final*/ ModelCacheUnloadBufManager unloadManager;

    // For instance-instance communications
    // The different clients have different routers but share underlying conn pool etc
    private /*final*/ BaseModelMeshService.Iface runtimeClient, cacheMissClient, directClient;

    protected String instanceId; // this instanceid, set in initialize()

    protected final long instanceStartTime; // absolute time that this instance started

    protected long longVersion; // set in initialize(), derived from the instance's version

    protected boolean sendDestinationId = true;

    // This is used only in latency-based autoscaling mode.
    // 0 means default which is currently max(16, maxConc * 8)
    //TODO add more explanation here
    protected /*final*/ int maxInflightPerModelCopy;

    // This is used only in latency-based autoscaling mode and is experimental
    protected /*final*/ long dynamicRpmScaleConstant; // = 600_000 * percentage; default percentage is 90

    // Per-model load threshold for when to consider scaling beyond 2 copies
    // (applies to model's load in this instance only)
    // Dynamically configurable via the scaleup_rpm_threshold kv parameter,
    // default is MM_SCALEUP_RPM_THRESHOLD or if not set then DEFAULT_SCALEUP_RPM constant.
    // Intentionally non-volatile for performance
    protected int scaleUpRpmThreshold = getDefaultScaleupRpms();

    // this tracks the count and recent rate of model invocations for this instance as a whole
    // OR cumulative prediction request processing time
    protected /*final*/ RateTracker invokeCounter;

    // this tracks count, average, and variance of time taken to load models, per model type.
    // the field/map itself is copy-on-write
    protected volatile Map<String, TimeStats> loadTimeStats = Collections.emptyMap();

    // if true (default) will log every model invocation at INFO level (otherwise DEBUG level)
    protected boolean logModelInvocations;

    static final Weigher<CacheEntry<?>> weigher = CacheEntry::getWeight;

    // this is the number of units which failed loads take up in the cache
    // the entry comprises just the CacheEntry object plus the corresponding exception
    protected static final int FAILED_WEIGHT = 1;

    protected /*final*/ ModelLoader<?> loader;

    protected /*final*/ ExecutorService loadingPool;
    final BlockingQueue<Runnable> loadingQueue;

    // this is a count of the number of loads in progress
    // including those queued, waiting, and running
    private final AtomicInteger loadingCount = new AtomicInteger();

    protected final ListeningScheduledExecutorService taskPool;

    private final boolean isExternal = this instanceof SidecarModelMesh;

    protected /*final*/ boolean limitModelConcurrency;

    // this is set to true post-initialization after verifying that there are
    // no other terminating pods in the same model-mesh cluster, and then
    // never reverts to false
    protected boolean reportReady;

    private RuntimeContainersPreStopServer preStopNettyServer;
    public static final String PRESTOP_SERVER_PORT_ENV_VAR = "MM_PRESTOP_WAIT_PORT";
    protected final int PRESTOP_SERVER_PORT;

    {
        String preStopPort = getStringParameter(PRESTOP_SERVER_PORT_ENV_VAR, null);

        try {
            PRESTOP_SERVER_PORT = preStopPort == null ? 8090 : Integer.parseInt(preStopPort);
        } catch (NumberFormatException nfe) {
            throw new NumberFormatException(
                    PRESTOP_SERVER_PORT_ENV_VAR + " is not a valid port number: " + preStopPort);
        }
    }

    private PayloadProcessor initPayloadProcessor() {
        String payloadProcessorsDefinitions = getStringParameter(MM_PAYLOAD_PROCESSORS, null);
        logger.info("Parsing PayloadProcessor definition '{}'", payloadProcessorsDefinitions);
        if (payloadProcessorsDefinitions != null && payloadProcessorsDefinitions.length() > 0) {
            List<PayloadProcessor> payloadProcessors = new ArrayList<>();
            for (String processorDefinition : payloadProcessorsDefinitions.split(" ")) {
                try {
                    URI uri = URI.create(processorDefinition);
                    String processorName = uri.getScheme();
                    PayloadProcessor processor = null;
                    String modelId = uri.getQuery();
                    String method = uri.getFragment();
                    if ("http".equals(processorName)) {
                        processor = new RemotePayloadProcessor(uri);
                    } else if ("logger".equals(processorName)) {
                        processor = new LoggingPayloadProcessor();
                    }
                    if (processor != null) {
                        MatchingPayloadProcessor p = MatchingPayloadProcessor.from(modelId, method, processor);
                        payloadProcessors.add(p);
                        logger.info("Added PayloadProcessor {}", p.getName());
                    }
                } catch (IllegalArgumentException iae) {
                    logger.error("Unable to parse PayloadProcessor URI definition {}", processorDefinition);
                }
            }
            return new AsyncPayloadProcessor(new CompositePayloadProcessor(payloadProcessors), 1, MINUTES,
                                             Executors.newScheduledThreadPool(getIntParameter(MM_PAYLOAD_PROCESSORS_THREADS, 2)),
                                             getIntParameter(MM_PAYLOAD_PROCESSORS_CAPACITY, 64));
        } else {
            return null;
        }
    }

    /* ---------------------------------- initialization --------------------------------------------------------- */

    @Override
    public Map<String, String> provideInstanceMetadata() {
        return Collections.singletonMap(KV_STORE_TYPE_META, getKvStoreType());
    }

    protected String getKvStoreType() {
        return kvUtilsFactory.getKvStoreType();
    }

    /**
     * Implemented by subclasses - perform service-specific startup operations
     * and return runtime parameters for this instance.
     */
    protected abstract LocalInstanceParameters startup() throws Exception;

    /**
     * Implemented by subclasses
     */
    protected abstract ModelLoader<?> getLoader();

    private final Future<Boolean> kvStoreVerifyFuture; // used only during startup

    public ModelMesh() throws Exception {
        instanceStartTime = currentTimeMillis();
        readOnlyMode = "true".equals(System.getenv(KV_READ_ONLY_ENV_VAR));
        if (readOnlyMode) {
            logger.warn("RUNNING IN READ-ONLY KV-STORE MODE");
        }

        kvUtilsFactory = KVUtilsFactory.getDefaultFactory();

        // Fail-fast if kv store is not available at startup
        // (to catch invalid etcd configuration for example)
        kvStoreVerifyFuture = kvUtilsFactory.verifyKvStoreConnection();

        // priority queue enables prioritization of
        // waiting runtime requests over preemptive loads
        loadingQueue = new PriorityBlockingQueue<>(11);

        ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(TASK_THREADS,
                ThreadPoolHelper.threadFactory("mm-task-thread-%d"));
        stpe.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        taskPool = MoreExecutors.listeningDecorator(stpe);
    }

    static final Pattern VERSION_PATT = Pattern.compile(".+-(\\d{8})-(\\d+)");
    final SimpleDateFormat VERSION_SDF = new SimpleDateFormat("yyyyMMdd") {
        { setTimeZone(TimeZone.getTimeZone("GMT")); }
    };

    static final Pattern VERSION_PATT_OLD = Pattern.compile("(\\d{8}-\\d{4})(?:-(\\d+))?");
    final SimpleDateFormat VERSION_SDF_OLD = new SimpleDateFormat("yyyyMMdd-HHmm") {
        { setTimeZone(TimeZone.getTimeZone("GMT")); }
    };

    @SuppressWarnings("unchecked")
    @Override
    protected final TProcessor initialize() throws Exception {
        ServiceDeploymentInfo deploymentInfo = getInstanceDeploymentInfo();
        instanceId = deploymentInfo.getInstanceId();
        if (instanceId == null || instanceId.isEmpty()) {
            throw new Exception("Invalid instanceId (null or empty)");
        }
        if (instanceId.indexOf(',') >= 0) {
            throw new Exception("Invalid instanceId (contains ,)");
        }
        excludeThisInstance = Collections.singletonList(instanceId);
        final String serviceName = deploymentInfo.getServiceName(), slotName = serviceName; //TODO
        final String serviceVers = deploymentInfo.getServiceVersion();
        logger.info("Service name is " + serviceName + ", Instance ID is "
                    + instanceId + ", Version is " + serviceVers);

        Class<? extends BaseModelMeshService.Iface> iface = null;
        for (Class<?> ifc : getClass().getInterfaces()) {
            if (BaseModelMeshService.Iface.class.isAssignableFrom(ifc)) {
                iface = (Class<? extends BaseModelMeshService.Iface>) ifc;
                break;
            }
        }
        if (iface == null) {
            throw new Exception("Must implement subinterface of BaseModelMeshService.Iface");
        }

        // Validate some constants which can be overridden via system properties
        if (SECOND_COPY_MAX_AGE_SECS < SECOND_COPY_MIN_AGE_SECS
            || SECOND_COPY_MIN_AGE_SECS < 0) {
            throw new Exception("Invalid value for overridden second-copy trigger age range");
        }

        // convert version string to long representation
        long lv = -1;
        if (serviceVers != null) {
            Matcher m = VERSION_PATT.matcher(serviceVers);
            if (m.matches()) try {
                lv = VERSION_SDF.parse(m.group(1)).getTime();
                if (m.group(2) != null) lv += Integer.parseInt(m.group(2));
            } catch (ParseException pe) { }
            else {
                m = VERSION_PATT_OLD.matcher(serviceVers);
                if (m.matches()) try {
                    lv = VERSION_SDF_OLD.parse(m.group(1)).getTime();
                    if (m.group(2) != null) lv += Integer.parseInt(m.group(2));
                } catch (ParseException pe) { }
            }

            if (lv >= 0) {
                this.longVersion = lv;
                logger.info("Numeric framework version is: " + lv);
            } else {
                logger.warn("Unrecognized service version format: " + serviceVers);
            }
        } else {
            logger.warn("No service version provided");
        }

        String tasConfigPath = ZKPaths.makePath(KV_STORE_PREFIX, slotName, "config");
        String registryPath = ZKPaths.makePath(KV_STORE_PREFIX, slotName, "registry");
        String vModelsPath = ZKPaths.makePath(KV_STORE_PREFIX, slotName, "vmodels");
        String instanceTablePath = ZKPaths.makePath(KV_STORE_PREFIX, slotName, "instances");
        String instanceNodePath = ZKPaths.makePath(instanceTablePath, instanceId);

        config = kvUtilsFactory.newDynamicConfig(tasConfigPath);

        // for consistency, get heap stats prior to loading registries
        MemoryMXBean mmxbean = ManagementFactory.getMemoryMXBean();
        mmxbean.gc();
        MemoryUsage heap = mmxbean.getHeapMemoryUsage(), nonheap = mmxbean.getNonHeapMemoryUsage();
        logger.info("Heap mem info: " + heap);
        logger.info("Non-heap mem info: " + nonheap);
        logger.info("Max heap = " + gb(heap.getMax()) + ", baseline overhead = " + (heap.getUsed() / Mi) + "MiB");

        instanceZone = System.getenv(ZONE_ENV_VAR);

        // use env var if set (typically set to kube node name or ip); otherwise our ip address
        String loc = System.getenv(LOCATION_ENV_VAR);
        instanceLocation = loc != null ? loc : deploymentInfo.getPublicAddress();

        logger.info("Instance zone = " + (instanceZone != null ? instanceZone : "<not-set>")
                    + ", location = " + instanceLocation);

        String labels = System.getenv(LABELS_ENV_VAR);
        if (labels != null) {
            labels = labels.trim();
            if (labels.isEmpty()) {
                logger.warn(LABELS_ENV_VAR + " is set to empty value");
            } else {
                instanceLabels = labels.split(",");
                for (int i = 0; i < instanceLabels.length; i++) {
                    instanceLabels[i] = instanceLabels[i].trim();
                }
                Arrays.sort(instanceLabels);
                logger.info(instanceLabels.length + " instance labels configured: "
                            + Arrays.toString(instanceLabels));
            }
        }
        if (instanceLabels == null) {
            logger.info("NO instance labels configured");
        }

        kvTable = kvUtilsFactory.newKVTable(registryPath, REGISTRY_BUCKETS);

        registry = kvTable.getView(new JsonSerializer<>(ModelRecord.class), 1);
        registry.addListener(false, this); // for deletion cleanup and metrics tracking

        // start registry async prior to other potentially time-consuming operations,
        // so that it will be populated in parallel
        final long beforeRegistryStart = nanoTime();
        ListenableFuture<Boolean> regStartFut = kvTable.start();
        Futures.addCallback(regStartFut, (FutureListener<Boolean>) (v, t) -> {
            if (t == null) {
                logger.info("Model registry load took " + (nanoTime() - beforeRegistryStart) / G
                            + "secs for " + kvTable.getCountAtLevel(1) + " models");
            }
        }, directExecutor());

        // vmodels only supported in grpc case
        if (GRPC_PORT > 0) try {
            vModelManager = new VModelManager(this, vModelsPath);
        } catch (UnsupportedOperationException uoe) {
            logger.warn("VModels not supported with kv-store type " + kvUtilsFactory.getKvStoreType());
        }
        ListenableFuture<Boolean> vmmStartFut = vModelManager != null ? vModelManager.start()
                : Futures.immediateFuture(Boolean.TRUE);

        Future<Boolean> configStartFuture = config.startAsync();

        try {
            kvStoreVerifyFuture.get(20, SECONDS);
            configStartFuture.get(1, MINUTES);
        } catch (Exception e) {
            Throwable t = e instanceof ExecutionException ? e.getCause() : e;
            throw new Exception("Problem connecting to " + kvUtilsFactory.getKvStoreType(), t);
        }

        // don't start if slot is disabled
        String disable = config.get(DISABLE_SLOT_PARAM);
        if (disable != null && !"false".equals(disable)) {
            throw new Exception("Slot " + slotName + " is disabled in kv-store of type "
                                + kvUtilsFactory.getKvStoreType());
        }

        initializeDynamicConfigParameters();

        // this must be done after config initialization
        LocalInstanceParameters runtimeParams = startup();

        // get subservice-specific model loader
        loader = getLoader();

        loadingThreads = runtimeParams.getMaxLoadingConcurrency();
        if (loadingThreads < 1) {
            throw new Exception("Model loading concurrency must be >= 1");
        }
        logger.info("Maximum model loading concurrency set to " + loadingThreads);

        defaultModelSizeUnits = runtimeParams.getDefaultModelSizeUnits();
        if (defaultModelSizeUnits < 1) {
            throw new Exception("Default model size must be >= 1 unit");
        }
        logger.info("Default model size set to " + defaultModelSizeUnits + " units ("
                    + mb(defaultModelSizeUnits * UNIT_SIZE, "MiB") + ")");

        loadTimeoutMs = runtimeParams.getModelLoadTimeoutMs();
        if (loadTimeoutMs < 100L) {
            throw new Exception("Loading timeout must be >= 100 ms");
        }
        logger.info("Model loading timeout set to " + loadTimeoutMs + "ms");

        defaultAssumeLoadedAfterMs = Math.max(3000L, loadTimeoutMs / 3);

        minChurnAgeMs = Long.getLong("tas.min_churn_age_ms", defaultAssumeLoadedAfterMs * 10L); // 10 mins default
        logger.info("Minimum churn age set to " + minChurnAgeMs + "ms");

        acceptedModelTypes = runtimeParams.getAcceptedModelTypes();
        logger.info("Accepted model types: "
                    + (acceptedModelTypes != null ? acceptedModelTypes.toString() : "(no restriction)"));

        limitModelConcurrency = runtimeParams.isLimitModelConcurrency();
        logger.info("Limit model concurrency: " + limitModelConcurrency);

        invokeCounter = new RateTracker(limitModelConcurrency);

        if (limitModelConcurrency) {
            String maxInflightParam = System.getenv(MAX_INFLIGHT_PER_COPY_ENV_VAR);
            if (maxInflightParam != null) {
                maxInflightPerModelCopy = Integer.parseInt(maxInflightParam);
            }
            String val = maxInflightPerModelCopy == 0 ? "default"
                    : (maxInflightPerModelCopy < 0 ? "unlimited" : "" + maxInflightPerModelCopy);
            logger.info("Max inflight requests per local model set to " + val);

            String concScaleThresholdPercentage = System.getenv(CONC_SCALEUP_BANDWIDTH_PCT_ENV_VAR);
            long pct = 90; // default is 90%
            if (concScaleThresholdPercentage != null) {
                try {
                    pct = Integer.parseInt(concScaleThresholdPercentage);
                    if (pct < 10 || pct > 200) {
                        throw new IllegalArgumentException("Out of range");
                    }
                } catch (IllegalArgumentException nfe) {
                    throw new Exception("Invalid value provided for " + CONC_SCALEUP_BANDWIDTH_PCT_ENV_VAR
                                        + " env var, must be in range [10, 200]: " + concScaleThresholdPercentage);
                }
            }
            logger.info("Latency-based autoscaling bandwidth threshold percentage set to " + pct);
            dynamicRpmScaleConstant = 600_000L * pct / 100;
        }

        long capacity = setupMemory(runtimeParams, heap), capUnits = capacity / UNIT_SIZE;
        long effectiveCapUnits = capUnits;
        runtimeCache = new ConcurrentLinkedHashMap.Builder<String, CacheEntry<?>>()
                .concurrencyLevel(2)
                .initialCapacity(64)
                .maximumWeightedCapacity(capUnits)
                .weigher(weigher).listener(this)
                .build();

        // if explicit model unloading is required, set up the accounting of this
        // (special placeholder entry in the cache whose weight is adjusted dynamically)
        if (loader.requiresUnload()) {
            // reserve part of the cache for unloads to avoid blocking new loads in most cases
            long cacheCapacity = runtimeCache.capacity();
            int lowerBound = Math.toIntExact(cacheCapacity / 100);
            int upperBound = Math.toIntExact(cacheCapacity / 10);
            int unitsToReserve = loadingThreads * defaultModelSizeUnits / (loadingThreads <= 2 ? 2 : 4);

            int unloadsReservedSizeUnits = Math.max(Math.min(unitsToReserve, upperBound), lowerBound);
            unloadManager = new ModelCacheUnloadBufManager(this, runtimeCache, unloadsReservedSizeUnits);
            effectiveCapUnits -= unloadsReservedSizeUnits;
            logger.info("Explicit unloading ENABLED, reserved cache space = " + unloadsReservedSizeUnits
                        + " units (" + mb(unloadsReservedSizeUnits * UNIT_SIZE, "MiB") + ")");
        } else {
            logger.info("Explicit unloading DISABLED");
        }
        logger.info("Total model capacity is " + mb(capacity) + " (" + capUnits + " units)");
        logger.info("Effective model capacity is " + mb(effectiveCapUnits * UNIT_SIZE)
            + " (" + effectiveCapUnits + " units)");

        // calculate "full" threshold
        //TODO this should probably be made more dynamic and/or model-type/situation specific
        int min = defaultModelSizeUnits * (unloadManager != null || loadingThreads <= 1 ? 1 : 2);
        int target = Math.min(defaultModelSizeUnits * loadingThreads, (int) (capUnits / 20));
        minSpaceUnits = Math.max(min, target);
        logger.info("Instance considered full if available space < " + minSpaceUnits + " units ("
                    + mb(minSpaceUnits * UNIT_SIZE) + ")");

        // needed before instancetable listener is added
        clusterState = new ConcurrentSkipListSet<>(PLACEMENT_ORDER); // comparator requires loadingThreads to be set

        // must be set before instancetable listener is set up
        typeConstraints = TypeConstraintManager.get(instanceId, clusterState, this::isFull); // null if not configured

        if (typeConstraints == null && instanceLabels != null) {
            logger.warn(LABELS_ENV_VAR + " is set but no type constraints are configured");
        }

        instanceTable = kvUtilsFactory.newKVTable(instanceTablePath, 0);
        instanceInfo = instanceTable.getView(INST_REC_SERIALIZER, 1);
        instanceInfo.addListener(false, instanceTableListener());

        byte[] initData = INST_REC_SERIALIZER.serialize(getFreshInstanceRecord());
        myNode = kvUtilsFactory.newSessionNode(instanceNodePath, initData);

        long beforeStart = nanoTime();
        instanceTable.start(2L, MINUTES);
        logger.info("Instance table load took " + (nanoTime() - beforeStart) / G + "secs for " + instanceInfo.getCount()
                    + " instances");

        loadingPool = new ThreadPoolExecutor(loadingThreads, loadingThreads, 0L, MILLISECONDS, loadingQueue,
                ThreadPoolHelper.threadFactory("mm-loader-thread-%d"));

        initializeClusterInternalLitelinksClients(iface, serviceName);

        // throws exception if running duplicate or instance with incompatible version is found
        // (must be done after initializing internal clients)
        checkForDuplicateInstanceIdOrIncompatibleVersion(deploymentInfo);

        // here wait for model registry population to complete
        Futures.allAsList(regStartFut, vmmStartFut).get(5L, MINUTES);

        // only publish our instance node after other init is complete
        myNode.start();

        establishBackgroundTasks();

        // Setup metrics
        metrics = setUpMetrics();
        assert metrics != null;

        metricsSetup = true; // volatile

        // keeping leading underscore for now to maintain compatibility with existing dashboards
        String leaderLatchId = "_" + instanceId;

        String llatchPath = ZKPaths.makePath(KV_STORE_PREFIX, slotName, "leaderLatch");
        leaderLatch = kvUtilsFactory.newLeaderElection(llatchPath, leaderLatchId);
        leaderLatch.addListener(this::leaderChange);
        logger.info("Starting leaderlatch, local id is \"" + leaderLatch.getId() + "\"");
        leaderLatch.start();

        try {
            StaticModelRegistration.registerAndVerify(this, vModelManager);
        } catch (Exception e) {
            throw new Exception("Failed to register/verify statically configured models", e);
        }

        // start external grpc server if configured
        if (GRPC_PORT > 0) {
            if (!isExternal) {
                throw new Exception("gRPC interface only supported for \"sidecar\"" + " model-meshes; un-set "
                                    + GRPC_PORT_ENV_VAR + " env var");
            }

            String privateKeyPath = System.getenv(TLS_PRIV_KEY_PATH_ENV_VAR);
            if (privateKeyPath == null) privateKeyPath = System.getenv(TLS_PRIV_KEY_PATH_ENV_VAR_OLD);
            if (privateKeyPath == null) privateKeyPath = System.getenv(TLS_PRIV_KEY_PATH_ENV_VAR_OLD2);
            File privateKeyFile = privateKeyPath != null ? new File(privateKeyPath) : null;

            String privateKeyPassphrase = System.getenv(TLS_PRIV_KEY_PASSPHRASE_ENV_VAR);
            if (privateKeyPassphrase == null) privateKeyPassphrase = System.getenv(TLS_PRIV_KEY_PASSPHRASE_ENV_VAR_OLD);

            String keyCertPath = System.getenv(TLS_KEY_CERT_PATH_ENV_VAR);
            if (keyCertPath == null) keyCertPath = System.getenv(TLS_KEY_CERT_PATH_ENV_VAR_OLD);
            if (keyCertPath == null) keyCertPath = System.getenv(TLS_KEY_CERT_PATH_ENV_VAR_OLD2);
            File keyCertFile = keyCertPath != null ? new File(keyCertPath) : null;

            String clientAuthStr = System.getenv(TLS_CLIENT_AUTH_ENV_VAR);
            ClientAuth clientAuth = ClientAuth.NONE;
            if (clientAuthStr != null) {
                try {
                    clientAuth = ClientAuth.valueOf(clientAuthStr.toUpperCase());
                } catch (IllegalArgumentException e) {
                    throw new Exception("Unrecognized value \" + clientAuthStr + \"for " + TLS_CLIENT_AUTH_ENV_VAR
                            + " env var, must be one of " + Arrays.toString(ClientAuth.values()));
                }
            }

            String caCertPath = System.getenv(TLS_TRUST_CERT_PATH_ENV_VAR);
            // Support single path or comma-separated list
            File[] caCertFiles = caCertPath == null ? null :
                    Stream.of(caCertPath.split(",")).map(File::new).toArray(File[]::new);

            int maxGrpcMessageSize = getMaxGrpcMessageSize();
            int maxGrpcHeadersSize = getMaxGrpcHeadersSize();

            logger.info("Configuring external model-mesh grpc server");
            if (privateKeyPath == null) {
                logger.warn("TLS NOT configured for external grpc server");
                if (clientAuth != ClientAuth.NONE) {
                    throw new Exception("TLS client-auth set to " + clientAuth + " without other required TLS config");
                }
            } else {
                logger.info("TLS configured for external grpc server, privateKeyPath=" + privateKeyPath
                        + ", keyCertPath=" + keyCertPath + ", clientAuth=" + clientAuth
                        + (caCertPath != null ? ", caCertPath=" + caCertPath : ""));
            }
            logger.info("Maximum grpc request message size set to " + maxGrpcMessageSize + " bytes");
            if (maxGrpcHeadersSize >= 0) {
                logger.info("Maximum grpc request headers size set to " + maxGrpcHeadersSize + " bytes");
            }

            long maxGrpcConnectionAge = getMaxGrpcConnectionAge();
            long maxGrpcConnectionAgeGrace = getMaxConnectionAgeGrace();
            if (maxGrpcConnectionAge < Long.MAX_VALUE) {
                logger.info("Maximum grpc connection age set to " + maxGrpcConnectionAge + " seconds");
            }
            if (maxGrpcConnectionAgeGrace < Long.MAX_VALUE) {
                logger.info("Maximum grpc connection age grace set to " + maxGrpcConnectionAgeGrace + " seconds");
            }

            LogRequestHeaders logHeaders = LogRequestHeaders.getConfiguredLogRequestHeaders();
            PayloadProcessor payloadProcessor = initPayloadProcessor();

            grpcServer = new ModelMeshApi((SidecarModelMesh) this, vModelManager, GRPC_PORT, keyCertFile, privateKeyFile,
                    privateKeyPassphrase, clientAuth, caCertFiles, maxGrpcMessageSize, maxGrpcHeadersSize,
                    maxGrpcConnectionAge, maxGrpcConnectionAgeGrace, logHeaders, payloadProcessor);
        }

        if (grpcServer != null) {
            logger.info("Starting external model-mesh grpc server on port " + GRPC_PORT);
            grpcServer.start();
        }

        // log netty buffer metrics
        logger.info("Netty PooledByteBufAllocator metrics after init: " + PooledByteBufAllocator.DEFAULT.metric());

        this.preStopNettyServer = new RuntimeContainersPreStopServer(PRESTOP_SERVER_PORT);

        return super.initialize();
    }

    // We no longer use a shared threadpool between external gRPC request threads and internal litelinks request threads
    // This is because we would like external request threads to be bounded, but sharing a bounded pool risks
    // a distributed deadlock situation. Since we are bounding the external req threads, it's OK for the internal
    // request threadpool to be unbounded (litelinks default)
//    @Override
//    public ExecutorService provideRequestExecutor() {
//        return grpcServer != null ? grpcServer.getRequestThreads() : super.provideRequestExecutor();
//    }

    // "type" or "type:p1=v1;p2=v2;...;pn=vn"
    private static final Pattern METRICS_CONFIG_PATT = Pattern.compile("([a-z;]+)(:\\w+=[^;]+(?:;\\w+=[^;]+)*)?");
    // "metric_name" or "metric:name;l1=v1,l2=v2,...,ln=vn,"
    private static final Pattern CUSTOM_METRIC_CONFIG_PATT = Pattern.compile("([a-z_:]+);(\\w+=[^;]+(?:;\\w+=[^,]+)*)?");

    private static Metrics setUpMetrics() throws Exception {
        if (System.getenv("MM_METRICS_STATSD_PORT") != null || System.getenv("MM_METRICS_PROMETHEUS_PORT") != null) {
            logger.warn("MM_METRICS_STATSD_PORT and MM_METRICS_PROMETHEUS_PORT env variables are deprecated"
                        + " and have no effect, use " + MMESH_METRICS_ENV_VAR + " instead");
        }
        String metricsConfig = getStringParameter(MMESH_METRICS_ENV_VAR, null);
        if (metricsConfig == null) {
            logger.info("{} env var not set, defaulting to emit Sysdig StatsD metrics to port 8126",
                    MMESH_METRICS_ENV_VAR);
            metricsConfig = "statsd:port=8126";
        } else {
            logger.info("{} set to \"{}\"", MMESH_METRICS_ENV_VAR, metricsConfig);
        }

        Matcher matcher = METRICS_CONFIG_PATT.matcher(metricsConfig);
        if (!matcher.matches()) {
            throw new Exception("Invalid metrics configuration provided in env var " + MMESH_METRICS_ENV_VAR + ": \""
                                + metricsConfig + "\"");
        }
        String type = matcher.group(1);
        String paramString = matcher.group(2);
        Map<String, String> params;
        if (paramString == null) {
            params = Collections.emptyMap();
        } else {
            params = new HashMap<>();
            for (String parm : paramString.substring(1).split(";")) {
                String[] kv = parm.split("=");
                params.put(kv[0], kv[1]);
            }
        }
        String infoMetricConfig = getStringParameter(MMESH_CUSTOM_ENV_VAR, null);
        Map<String, String> infoMetricParams;
        if (infoMetricConfig == null) {
            logger.info("{} returned null", MMESH_CUSTOM_ENV_VAR);
            infoMetricParams = null;
        } else {
            logger.info("{} set to \"{}\"", MMESH_CUSTOM_ENV_VAR, infoMetricConfig);
            Matcher infoMetricMatcher = CUSTOM_METRIC_CONFIG_PATT.matcher(infoMetricConfig);
            if (!infoMetricMatcher.matches()) {
                throw new Exception("Invalid metrics configuration provided in env var " + MMESH_CUSTOM_ENV_VAR + ": \""
                                    + infoMetricConfig + "\"");
            }
            String infoMetricName = infoMetricMatcher.group(1);
            String infoMetricParamString = infoMetricMatcher.group(2);
            infoMetricParams = new HashMap<>();
            infoMetricParams.put("metric_name", infoMetricName);
            for (String infoMetricParam : infoMetricParamString.substring(0).split(",")) {
                String[] kv = infoMetricParam.split("=");
                String value = System.getenv(kv[1]);
                if (value == null) {
                    throw new Exception("Env var " + kv[1] + " is unresolved in " + MMESH_CUSTOM_ENV_VAR + ": \""
                            + infoMetricConfig + "\"");
                }
                infoMetricParams.put(kv[0], value);
            }
        }
        try {
            switch (type.toLowerCase()) {
            case "statsd":
                return new Metrics.StatsDMetrics(params);
            case "prometheus":
                return new Metrics.PrometheusMetrics(params, infoMetricParams);
            case "disabled":
                logger.info("Metrics publishing is disabled (env var {}={})", MMESH_METRICS_ENV_VAR, metricsConfig);
                return Metrics.NO_OP_METRICS;
            default:
                throw new Exception("Invalid metrics type \"" + type + "\"");
            }
        } catch (Exception e) {
            logger.error("Error while configuring metrics, they will be disabled", e);
        }
        return Metrics.NO_OP_METRICS;
    }

    // called only once, from initialize()
    private void initializeDynamicConfigParameters() {
        // Watcher for changes to log4j level parameter
        config.addAndInvokeListener((key, value) -> {
            try {
                Configurator.setRootLevel(Level.valueOf(value));
                logger.info("Set debugging level to " + value);
            } catch (IllegalArgumentException e) {
                // Use default value of info, but also warn about the invalid key-value
                logger.warn("Invalid logging level " + value, e);
                Configurator.setRootLevel(Level.INFO);
            } catch (NullPointerException e) {
                logger.info("Logging level parameter deleted, reverting to INFO");
                Configurator.setRootLevel(Level.INFO);
            }
        }, Collections.singleton(LOGGER_LEVEL_PARAM));

        // Watcher for invocation logging parameter
        config.addAndInvokeListener((key, value) -> {
            // defaults to env var, then true
            boolean newValue = value != null ? Boolean.parseBoolean(value)
                    : "true".equalsIgnoreCase(System.getenv(LOG_EACH_INVOKE_ENV_VAR));
            logModelInvocations = newValue;
            logger.info(LOG_EACH_INVOCATION + " parameter now set to " + newValue);
        }, Collections.singleton(LOG_EACH_INVOCATION));

        // Watcher for changes to scale-up load threshold parameter
        config.addAndInvokeListener((key, value) -> {
            if (limitModelConcurrency) {
                if (value != null) {
                    logger.warn(SCALEUP_RPM_PARAM + " kv-store setting of " + value
                                + " will have no effect since model-mesh is in latency-based scaling mode");
                }
                return;
            }
            int val = -1;
            if (value == null) {
                scaleUpRpmThreshold = val = getDefaultScaleupRpms();
                logger.info(SCALEUP_RPM_PARAM + " kv-store param not set," + " reverting to " + val + "rpm");
            } else {
                try {
                    val = Integer.parseInt(value);
                    if (val >= 10) {
                        scaleUpRpmThreshold = val;
                        logger.info(SCALEUP_RPM_PARAM + " kv-store param set to " + value + "rpm");
                    }
                } catch (NumberFormatException nfe) { }
            }
            if (val < 10) {
                scaleUpRpmThreshold = getDefaultScaleupRpms();
                logger.warn("Invalid value \"" + value + "\"for " + SCALEUP_RPM_PARAM + " kv-store param, reverting to "
                            + scaleUpRpmThreshold + "rpm");
            }
        }, Collections.singleton(SCALEUP_RPM_PARAM));
    }

    protected static int getDefaultScaleupRpms() {
        return getIntParameter(SCALEUP_RPM_ENV_VAR, DEFAULT_SCALEUP_RPM);
    }

    protected static int getMaxGrpcMessageSize() {
        return getIntParameter(GRPC_MAX_MSG_SIZE_ENV_VAR, DEFAULT_GRPC_MAX_MSG_SIZE);
    }

    // returns -1 if not set
    protected static int getMaxGrpcHeadersSize() {
        return getIntParameter(GRPC_MAX_HEADERS_SIZE_ENV_VAR, -1);
    }

    protected static long getMaxGrpcConnectionAge() {
        return getLongParameter(GRPC_MAX_CONNECTION_AGE_SECS_ENV_VAR, Long.MAX_VALUE);
    }

    protected static long getMaxConnectionAgeGrace() {
        return getLongParameter(GRPC_MAX_CONNECTION_AGE_GRACE_SECS_ENV_VAR, Long.MAX_VALUE);
    }

    protected static String getStringParameter(String name, String defaultVal) {
        String value = System.getenv(name);
        return value != null ? value : System.getProperty(name, defaultVal);
    }

    protected static int getIntParameter(String name, int defaultVal) {
        String value = System.getenv(name);
        return value != null ? Integer.parseInt(value) : Integer.getInteger(name, defaultVal);
    }

    protected static long getLongParameter(String name, long defaultVal) {
        String value = System.getenv(name);
        return value != null ? Long.parseLong(value) : Long.getLong(name, defaultVal);
    }

    // called only once, from initialize()
    private void initializeClusterInternalLitelinksClients(Class<? extends BaseModelMeshService.Iface> iface,
            String serviceName) {
        int remoteTimeoutMillis = getRequestTimeoutMillis();
        runtimeClient = ThriftClientBuilder.newBuilder(iface)
                .withServiceName(serviceName)
                .withLoadBalancer(ForwardingLB::new)
                .withTimeout(remoteTimeoutMillis).build();
        cacheMissClient = ThriftClientBuilder.newBuilder(iface)
                .withServiceName(serviceName)
                .withLoadBalancer(CacheMissForwardingLB::new)
                .withTimeout(remoteTimeoutMillis).build();

        String privEndpoint = System.getenv(LitelinksEnvVariableNames.PRIVATE_ENDPOINT);
        if (privEndpoint == null || privEndpoint.trim().isEmpty()) {
            privEndpoint = System.getProperty(LitelinksSystemPropNames.PRIVATE_ENDPOINT);
        }
        boolean privateNetwork = privEndpoint != null && !privEndpoint.trim().isEmpty();

        if (!privateNetwork) {
            // Only send destination instance id if we're *not* in a private cluster AND
            // the option is configured via a zookeeper config parameter
            config.addAndInvokeListener((key, val) -> {
                // defaults to env var, then true
                sendDestinationId = val != null ? Boolean.parseBoolean(val)
                        : !"false".equalsIgnoreCase(System.getenv(SEND_DEST_ID_ENV_VAR));
                logger.info("send_destination_id kv configuration parameter set to: " + sendDestinationId);
            }, Collections.singleton(SEND_DEST_ID_PARAM));
        } else {
            // direct forwarding is only done if we're configured with both private + public endpoints
            directClient = ThriftClientBuilder.newBuilder(iface).withServiceName(serviceName)
                    .withLoadBalancer(() -> new IdBasedLoadBalancer() {
                        @SuppressWarnings("unchecked")
                        @Override
                        public ServiceInstanceInfo getNext(Object[] list, String method, Object[] args) {
                            String destInst = destinationInstance.get();
                            return destInst != null ? getMap(list).get(destInst) : null;
                        }
                    })
                    .withTimeout(remoteTimeoutMillis).build();
        }
    }

    /**
     * @return 0 for no timeout per remote litelinks request
     */
    protected int getRequestTimeoutMillis() {
        // default is load timeout
        return (int) loadTimeoutMs;
    }

    // called only once, from initialize()
    private void establishBackgroundTasks() {
        // this task publishes this instance's record at a fixed rate
        taskPool.scheduleWithFixedDelay(() -> {
            if (!shuttingDown) {
                try {
                    // only *actually* publishes if it has changed sufficiently
                    publishInstanceRecord(false, false);
                } catch (Throwable t) {
                    logger.error("Error publishing instance record", t);
                }
            }
        }, INSTANCE_REC_PUBLISH_FREQ_MS, INSTANCE_REC_PUBLISH_FREQ_MS, MILLISECONDS);

        // this task does pruning of local cache and global registry based on mutual state
        taskPool.scheduleAtFixedRate(janitorTask(), 1L, LOCAL_JANITOR_FREQ_SECS, SECONDS);

        // this task updates the request-rate for the whole instance every minute
        taskPool.scheduleWithFixedDelay(invokeCounter.reqCounterTask(), 3L, 60L, SECONDS);

        // this task checks per model req rates and triggers model scale-out beyond 2 copies if necessary
        taskPool.scheduleAtFixedRate(rateTrackingTask(), RATE_CHECK_INTERVAL_MS / 2, RATE_CHECK_INTERVAL_MS, MILLISECONDS);
    }

    private static final String MIN_PRIOR_VERSION = "20170201-0000";
    private static final String FIRST_INCOMPAT_VERSION = "20170515-0000";

    // called only once, from initialize()
    private void checkForDuplicateInstanceIdOrIncompatibleVersion(ServiceDeploymentInfo sdi) throws Exception {

        String ourKvStoreType = getKvStoreType();

        // check for incompatible existing version or duplicate instanceid
        for (ServiceInstanceInfo sii : ((LitelinksServiceClient) runtimeClient).getServiceInstanceInfo()) {
            String version = sii.getVersion();
            Matcher m = version == null ? null : VERSION_PATT_OLD.matcher(version);
            if (m == null || (m.matches() && MIN_PRIOR_VERSION.compareTo(m.group(1)) > 0)) {
                logger.error("Aborting start because another model-mesh instance was found in" + " this cluster ("
                        + sdi.getServiceName() + ") with an incompatible version");
                logger.error("Existing instance " + sii.getInstanceId() + " has version " + version
                        + ". The cluster must first be upgraded to a version between " + MIN_PRIOR_VERSION + " and "
                        + FIRST_INCOMPAT_VERSION + " before being upgraded to this one (" + sdi.getServiceVersion()
                        + ")");
                throw new Exception("Must upgrade cluster to version between " + MIN_PRIOR_VERSION + " and "
                        + FIRST_INCOMPAT_VERSION + " before upgrading to this one (" + sdi.getServiceVersion() + ")");
            }

            if (instanceId.equals(sii.getInstanceId())) {
                boolean reachable = false;
                try {
                    sii.testConnection(5000L);
                    reachable = true;
                } catch (InterruptedException ie) {
                    throw ie;
                } catch (Exception e) {
                    // if connection to duplicate instance fails, we can assume it is defunct
                    logger.warn("Found registration of another model-mesh instance with the same id " + instanceId
                                + " which appears to be unreachable");
                }
                if (reachable) {
                    // fail initialization if the other instance is reachable
                    logger.error("A model-mesh instance already exists in this cluster with the same id "
                                 + instanceId + " on host " + sii.getHost() + " with version " + sii.getVersion());
                    throw new Exception("An instance already exists with our id: " + instanceId);
                }
            }

            // also ensure that if the kv store type differs across the cluster that
            // we are in read-only mode (should only be applicable during one-time migration)
            if (!readOnlyMode && ourKvStoreType != null) {
                Map<String, String> meta = sii.getMetadata();
                String otherKvStoreType = meta != null ? meta.get(KV_STORE_TYPE_META) : null;
                otherKvStoreType = otherKvStoreType != null ? otherKvStoreType.split(":")[0] : null;
                if (otherKvStoreType != null && !"unknown".equals(otherKvStoreType)
                    && !ourKvStoreType.equals(otherKvStoreType)) {
                    logger.error("Instance " + sii.getInstanceId() + " using different kv store type "
                                 + otherKvStoreType + ". Must be in read-only mode for kv store migration");
                    throw new Exception("Inconsistent cluster KV store types (another instance is using "
                                        + otherKvStoreType + ")");
                }
            }
        }
    }

    // called only once, from initialize()
    protected static long setupMemory(LocalInstanceParameters runtimeParams, MemoryUsage heap) throws Exception {

        long capacityBytes;
        boolean useHeap = runtimeParams.isUseHeap();
        if (useHeap) {
            logger.info("Configured to use IN-PROCESS JVM HEAP to hold models");
            long staticOverheadMb = runtimeParams.getExtraStaticHeapUsageMiB();
            logger.info("Additional static heap overhead set to " + staticOverheadMb + "MiB");

            long maxHeap = heap.getMax(), youngSpace = staticOverheadMb * Mi;
            // reserve 20% for young generation if heap >= 8GiB
            if (maxHeap >= 8 * Gi) {
                youngSpace += (maxHeap * 20L) / 100L;
            } else {
                // increasing percentage as heap gets smaller, down to 50% of 512MiB heap = 256MiB
                youngSpace += ((2 * maxHeap) / 11) + (7 * Gi / 44);
            }
            // enforce minimum space for young gen to be 256MiB
            if (youngSpace < 256 * Mi) {
                youngSpace = 256 * Mi;
            }
//          else if(youngSpace > 4L *G) youngSpace = 4L *G;

            capacityBytes = heap.getMax() - (heap.getUsed() + youngSpace);
        } else {
            logger.info("Configured to hold models EXTERNALLY");
            capacityBytes = runtimeParams.getCapacityBytes();
        }

        // Enforce minimum capacity of 10 models of advertised default size, or 512 MiB,
        // whichever is smaller. Must also be at least as big as 2 models of advertised default size.
        long defaultModelSizeBytes = runtimeParams.getDefaultModelSizeUnits() * UNIT_SIZE;
        long minCapacityBytes = Math.max(2L * defaultModelSizeBytes, Math.min(512 * Mi, 10L * defaultModelSizeBytes));
        logger.info("Enforcing minimum model capacity of " + mb(minCapacityBytes));

        if (capacityBytes < minCapacityBytes) {
            if (useHeap) {
                throw new Exception("Heap sized too small (increase max from " + mb(heap.getMax()) + ")");
            }
            throw new Exception("Reported model capacity " + gb(capacityBytes)
                    + " too small relative to advertised default model size of " + mb(defaultModelSizeBytes));
        }

        logger.info("Model cache capacity = " + gb(capacityBytes));
        return capacityBytes;
    }

    KVTable.TableView<ModelRecord> getRegistry() {
        return registry;
    }

    KVUtilsFactory getKvUtilsFactory() {
        return kvUtilsFactory;
    }

    boolean isLeader() {
        LeaderElection le = leaderLatch;
        return le != null && le.isLeader();
    }

    /*
     * We don't begin to return READY until no other members of the same logical
     * model-mesh deployment are in a terminating state. We can still receive
     * internal requests and load/serve models, but won't receive external
     * requests since Kubernetes won't yet consider us to be ready.
     *
     * This is done to facilitate controlled rolling updates in conjunction with
     * the maxUnavailable and maxSurge deployment parameters, ensuring that
     * existing pods can be stopped in a staggered manner.
     *
     * This in turn is to ensure that there is sufficient time for model copies
     * to propagate to new instances, to avoid any disruption to ongoing inferencing
     * requests.
     */
    @Override
    protected boolean isReady() {
        if (abortStartup) {
            return false;
        }
        // called only post-initialization
        if (!reportReady) {
            for (Entry<String, InstanceRecord> instance : instanceInfo) {
                if (instance.getValue().isShuttingDown()) {
                    logger.info("Returning NOT READY to readiness probe since there"
                                + " is at least one other pod in the process of shutting down"
                                + " (including " + instance.getKey() + ")");
                    return false;
                }
            }
            logger.info("Returning READY to readiness probe"
                        + " (did not find any other pods in terminating state)");
            reportReady = true;
        }

        return true;
    }

    /* -------------------------- "fail-fast" startup probation period feature -------------------- */

    protected volatile boolean abortStartup;    // flag used to abort startup in case of unexpected model loading failures
    protected AtomicInteger btspSuccessCount; // count of all succeeded load while bootstrap
    protected AtomicInteger btspFatalCount;   // count of all fatal failures load while bootstrap
    protected AtomicInteger btspFailureCount; // count of all general failures load while bootstrap

    {
        String btspClearanceStr = System.getenv(BOOTSTRAP_CLEARANCE_PERIOD_ENV_VAR);
        if (!Strings.isNullOrEmpty(btspClearanceStr)) {
            BOOTSTRAP_CLEARANCE_PERIOD_MS = Long.parseLong(btspClearanceStr);
        }

        boolean failfastUpgradeEnabled = !"false".equalsIgnoreCase(
                System.getenv(FAILFAST_UPGRADE_ENV_VAR));
        if (failfastUpgradeEnabled) {
            btspSuccessCount = new AtomicInteger();
            btspFatalCount = new AtomicInteger();
            btspFailureCount = new AtomicInteger();
        }
    }

    void processStartupFailFastSuccess() {
        AtomicInteger tempSuccessCount = btspSuccessCount;  // temp reference to avoid race condition
        if (tempSuccessCount == null) {
            return;
        }
        if (currentTimeMillis() - instanceStartTime < BOOTSTRAP_CLEARANCE_PERIOD_MS) {
            tempSuccessCount.incrementAndGet();
        } else {
            disposeBootstrapCounters();
        }
    }

    void processStartupFailFastFailure(ModelRecord mr) {
        if (btspSuccessCount == null) {
            return;
        }
        if (currentTimeMillis() - instanceStartTime < BOOTSTRAP_CLEARANCE_PERIOD_MS) {
            checkForBootstrapFailure(mr);
        } else {
            disposeBootstrapCounters();
        }
    }

    // Determine instance readiness while in bootstrap clearance period,
    // This is used to prevent instance to report ready immaturely, which will allow K8s rolling upgrade
    // to fail fast. See: https://github.ibm.com/watson-foundation-services/tsd-tracker/issues/4108
    private void checkForBootstrapFailure(ModelRecord mr) {
        AtomicInteger tempSuccessCount = btspSuccessCount, tempFailureCount = btspFailureCount,
                tempFatalCount = btspFatalCount;    // assign temp var to avoid race condition
        if (tempSuccessCount != null) {
            // true if this model failed loading in other instance 30min ago
            boolean failedBefore = mr.getLoadFailedInstanceIds().values().stream()
                    .anyMatch(loadStartTime ->
                            currentTimeMillis() - loadStartTime >= 30 * 60 * 1000);

            // true if this model was loaded successfully in other instance
            boolean succeededBefore = mr.getInstanceIds().values().stream()
                    .anyMatch(loadStartTime ->
                            currentTimeMillis() - loadStartTime >= loadTimeoutMs);

            if (succeededBefore) {
                // fatal: this model only failed on this instance
                int curFatalCount = tempFatalCount.incrementAndGet();
                if (curFatalCount > 2) {
                    abortStartup = true;
                    disposeBootstrapCounters();
                }
            } else if (!failedBefore) {
                // general: this model failed loading for first time
                double failures = tempFailureCount.incrementAndGet() + tempFatalCount.get(),
                        total = tempSuccessCount.get() + loadingCount.get() + failures;
                if (total > 20 && failures / total > 0.5) {
                    abortStartup = true;
                    disposeBootstrapCounters();
                }
            } // else: this model loading has previous failures, so ignore this failed
        }
    }

    private void disposeBootstrapCounters() {
        // dereference for gc
        btspSuccessCount = null;
        btspFailureCount = null;
        btspFatalCount = null;
    }

    /* --------------------------- instance table and cluster stats management ------------------------------ */

    private KVTable.Listener<InstanceRecord> instanceTableListener() {
        return typeConstraints == null ? this::handleInstanceTableChange
                : (type, key, record) -> typeConstraints.executor().execute(()
                -> handleInstanceTableChange(type, key, record));
    }

    /**
     * Get the stats corresponding to the cluster subset on which a given mdoel type can reside
     */
    ClusterStats typeSetStats(String modelType) {
        if (typeConstraints == null) {
            return clusterStats;
        }
        ClusterStats stats = typeConstraints.getTypeSetStats(modelType);
        return stats != null ? stats : clusterStats;
    }

    /**
     * Get the stats corresponding to the cluster subset to which this instance resides
     * <p>
     * The subset is determined by all of the instances which have the same <i>effective</i>
     * constraint w.r.t. which model types they can host
     */
    ClusterStats instanceSetStats() {
        return typeConstraints != null ? typeConstraints.getLocalInstanceSetStats() : clusterStats;
    }

    // This is just for (re)use in the method below, prior to updating the global volatile clusterStats field
    private final InstanceSetStatsTracker clusterStatsTracker = new InstanceSetStatsTracker(null, this::isFull);

    private int changeCounter;

    private void handleInstanceTableChange(EventType type, String key, InstanceRecord record) {
        if (logger.isDebugEnabled()) {
            logger.debug("IR listener event type=" + type + " key=" + key + " ir=" + record);
        }

        InstanceSetStatsTracker subsetStats = null;
        if (record != null) {
            if (record.isShuttingDown()) {
                type = ENTRY_DELETED;
            }
            if (typeConstraints != null) {
                subsetStats = typeConstraints.getStatsForLabels(record.getLabels());
            }
        }
        // Global and possibly type-subset stats are updated via deltas (an update to an existing instance
        // will result in adding new counts followed by subtracting the old ones), apart from the LRUs
        // which are re-calculated each time during the delete loop below
        boolean wasAddedToTc = false;
        Entry<String, InstanceRecord> keep = null;
        switch (type) {
        // not necessary, we receive ADDED events upon initialization
//          case INITIALIZED:
//              Iterators.addAll(clusterState, instanceInfo.entryIterator(1));
//              break;
        case ENTRY_ADDED:
            // fall-thru
        case ENTRY_UPDATED:
            if (record == null) {
                logger.warn("Unexpected null record in InstanceRecord listener "
                            + type + " event for instance " + key);
                return;
            }
            if (typeConstraints != null) {
                if (type == ENTRY_ADDED || subsetStats == null) {
                    // If applicable, we make sure to update the type constraints _prior_
                    // to updating the clusterState list, so that any placement decisions
                    // take the correct information into account
                    subsetStats = typeConstraints.instanceAdded(key, record.getLabels(), false);
                    wasAddedToTc = true;
                }
                record.prohibitedTypes = subsetStats.prohibitedTypesSet;
            }
            boolean added = clusterState.add(keep = Maps.immutableEntry(key, record));
            if (!added) {
                logger.warn("Identical instance record already exists in clusterState"
                            + " on update event for instance " + key);
                return; // no change
            }
            clusterStatsTracker.add(key, record);
            if (subsetStats != null) {
                subsetStats.add(key, record);
            }
            LeaderElection ll = leaderLatch;
            if (ll != null && ll.isLeader()) {
                missings.remove(key);
            }
            // fall-thru
        case ENTRY_DELETED:

            // Remove replaced/deleted entry (exclude the one just added in fall-through case)
            clusterStatsTracker.resetLru();
            if (subsetStats != null) {
                subsetStats.resetLru();
            }
            boolean existingWasRemoved = false;

            // Look for any with the same key
            for (Iterator<Entry<String, InstanceRecord>> it = clusterState.iterator(); it.hasNext(); ) {
                Entry<String, InstanceRecord> ent = it.next();
                InstanceRecord ir = ent.getValue();
                if (key.equals(ent.getKey()) && (keep == null || PLACEMENT_ORDER.compare(ent, keep) != 0)) {
                    clusterStatsTracker.remove(key, ir);
                    if (typeConstraints != null) {
                        if (subsetStats != null) subsetStats.remove(key, ir);
                        if (type == ENTRY_DELETED) typeConstraints.instanceRemoved(key, ir.getLabels());
                    }
                    if (type == ENTRY_DELETED) {
                        upgradeTracker.instanceRemoved(key, ir);
                    }
                    existingWasRemoved = true;
                    it.remove();
                } else {
                    clusterStatsTracker.addLru(ir.getLruTime());
                    if (subsetStats != null) {
                        subsetStats.addLru(ir.getLruTime());
                    }
                }
            }
            if (typeConstraints != null) {
                if (type != ENTRY_DELETED && !wasAddedToTc && !existingWasRemoved) {
                    // Safety-net in case this is actually a new addition but wasn't added above
                    typeConstraints.instanceAdded(key, record.getLabels(), true);
                }
                if (subsetStats != null) {
                    subsetStats.update();
                }
            }
            if (type != ENTRY_DELETED && existingWasRemoved) {
                upgradeTracker.instanceAdded(key, record);
            }
            clusterStats = clusterStatsTracker.update();

            if (type == ENTRY_DELETED && Objects.equals(instanceId, key)) {
                // If our record was deleted, might be after a reconnection (session expiry)
                publishInstanceRecordAsync();
            }
            // This will perform the upgradeTracker housekeeping once every 64 times that we pass here
            if ((changeCounter++ & 63) == 0) {
                upgradeTracker.doHousekeeping();
            }
        default:
            break;
        }
    }

    static class ClusterStats {
        final int instanceCount;
        final int modelCopyCount;
        final long totalCapacity, totalFree; // in units
        final long globalLru; // Long.MAX_VALUE if there is none

        public ClusterStats(long totalCapacity, long totalFree, long globalLru, int instanceCount, int modelCopyCount) {
            this.totalCapacity = totalCapacity;
            this.totalFree = totalFree;
            this.globalLru = globalLru;
            this.instanceCount = instanceCount;
            this.modelCopyCount = modelCopyCount;
        }

        @Override
        public String toString() {
            String lruStr = globalLru < 0L || globalLru == Long.MAX_VALUE ? "never"
                    : globalLru == 0 ? "0" : globalLru + " (" + readableTime(age(globalLru)) + " ago)";
            return "ClusterStats [instCount=" + instanceCount + ", modelCopyCount=" + modelCopyCount +
                   ", totalCapacity=" + totalCapacity + "u, totalFree=" + totalFree + "u, globalLru=" + lruStr + "]";
        }
    }

    /* ----------------------- model loading time stats -----------------------------------------------------------*/

    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<ModelMesh, Map> LOADSTATS_MAP_UPDATER
            = AtomicReferenceFieldUpdater.newUpdater(ModelMesh.class, Map.class, "loadTimeStats");

    final TimeStats loadingTimeStats(String modelType) {
        modelType = Strings.nullToEmpty(modelType);
        while (true) {
            Map<String, TimeStats> map = loadTimeStats;
            TimeStats stats = map.get(modelType);
            if (stats != null) {
                return stats;
            }
            Map<String, TimeStats> newMap = ImmutableMap.<String, TimeStats>builderWithExpectedSize(map.size() + 1)
                    .putAll(map).put(modelType, stats = new TimeStats(defaultAssumeLoadedAfterMs)).build();
            if (LOADSTATS_MAP_UPDATER.compareAndSet(this, map, newMap)) {
                return stats;
            }
        }
    }

    /* --------------------------------- local cache entry ------------------------------------------------------ */

    final CacheEntry<?> newInternalCacheEntry(String id, int weight) {
        CacheEntry<?> ce = new CacheEntry(id, weight);
        // timestamp is set to max long value so this will never be evicted
        runtimeCache.putIfAbsent(id, ce, Long.MAX_VALUE);
        return ce;
    }

    static String ceStateString(int st) {
        return st >= 0 && st < CE_STATE_STRS.length ? CE_STATE_STRS[st] : "UNRECOGNIZED(" + st + ")";
    }

    /**
     * This class holds a model in the cache. It also contains the logic
     * related to loading that model.
     */
    class CacheEntry<T> extends AbstractFuture<T> implements Runnable, Comparable<CacheEntry<?>> {

        protected final String modelId;
        private final ModelInfo modelInfo;
        final long loadTimestamp;

        // Records time of first transition into any of SIZING/ACTIVE/FAILED states
        long loadCompleteTimestamp;

        // These are used *only* by the rate tracking task and only when this is the only
        // copy of the model. They track prior iteration numbers of the rateTrackingTask
        // in which this model was observed to have been used.
        // Addition of a second copy is triggered when a usage is detected and one of
        // these prior recorded usage iterations falls within a particular range
        // (corresponding to roughly > 7 mins and < 40 mins ago).
        // earlierUseIteration <= lastUsedIteration is always true
        int earlierUseIteration = Integer.MIN_VALUE, lastUsedIteration = Integer.MIN_VALUE;

        private CacheEntry(String modelId, int weight) { // not used for "real" entries
            this.modelId = modelId;
            this.modelInfo = null;
            this.loadTimestamp = -1L;
            this.loadCompleteTimestamp = -1L;
            this.weight = weight;
            this.invokeCompletionCount = null;
        }

        CacheEntry(String modelId, ModelRecord mr, Throwable failure) { // for already-failed entries
            this(modelId, new ModelInfo(mr.getType(), mr.getModelPath())
                    .setEncKey(mr.getEncryptionKey()), failure);
        }

        CacheEntry(CacheEntry<?> replaceFailed) { // for updating loadTimestamp of already-failed entries
            this(replaceFailed.modelId, replaceFailed.modelInfo, replaceFailed.finalException());
        }

        private CacheEntry(String modelId, ModelInfo modelInfo, Throwable failure) { // for already-failed entries
            this.modelId = modelId;
            this.loadTimestamp = currentTimeMillis();
            this.modelInfo = modelInfo;
            this.weight = FAILED_WEIGHT;
            this.invokeCompletionCount = null;
            setFailed(failure);
        }

        public CacheEntry(String modelId, ModelRecord mr, long loadTimestamp, long priority) {
            this.modelId = modelId;
            this.loadTimestamp = loadTimestamp;
            this.modelInfo = new ModelInfo(mr.getType(), mr.getModelPath());
            this.modelInfo.setEncKey(mr.getEncryptionKey());
            this.weight = INSERTION_WEIGHT;
            this.priority = priority;
            this.invokeCompletionCount = recordInvokeCompletions() ? new LongAdder() : null;
        }

        // overridden to return false in MaxConcCacheEntry
        protected boolean recordInvokeCompletions() {
            return unloadManager != null;
        }

        final String modelType() {
            return modelInfo != null ? modelInfo.getServiceType() : null;
        }

        boolean inflightRequests() {
            return invokeCompletionCount != null && invokeCompletionCount.sum() < invokeCount.sum();
        }

        // -------------- invocation rate tracking -------------------

        private final LongAdder invokeCount = new LongAdder();

        // This is not used in concurrency-restricted mode
        // or if explicit unloads aren't required
        private final LongAdder invokeCompletionCount;

        // these two modified only by rate-tracking task
        private long lastInvokeCount;
        // last time that load average of this model exceeded
        // 3/4 of the scale-up req-load threshold
        // This variable is also used temporarily at creation
        // time, to record time spent in the loading queue
        private volatile long lastHeavyTime;

        void recordInvocation(int weight) {
            invokeCount.add(weight);
        }

        final long getAndResetIntervalCount() {
            long countNow = invokeCount.sum(), delta = countNow - lastInvokeCount;
            lastInvokeCount = countNow;
            return delta;
        }

        final void setLastHeavyTime(long millis) {
            lastHeavyTime = millis;
        }

        final long getLastHeavyTime() {
            return lastHeavyTime;
        }

        final long getIntervalCount() {
            return invokeCount.sum() - lastInvokeCount;
        }

        final long getRpm(long timeSinceLastCheck) {
            long countSinceLastTime = getIntervalCount();
            return countSinceLastTime == 0L ? 0L : (60_000L * countSinceLastTime) / timeSinceLastCheck;
        }

        final long getTotalInvocationCount() {
            return invokeCount.sum();
        }

        final long getApproxTotalInvocationCount() {
            return lastInvokeCount;
        }

        // -------------------- weight management ---------------------

        // This value specifies the entry's weight when inserted or replaced in the cache.
        // Starts at predicted weight, updated after successful/failed load completion.
        // Abs value of negative weight reflects prediction rather than "known" size.
        private int weight;

        public final int getWeight() {
            return Math.abs(weight);
        }

        /**
         * @param newWeight >0 means strong prediction,
         *                  absolute value of <0 means weak prediction
         */
        void updateWeight(int newWeight) {
            //TODO maybe ensure != 0
            Lock cacheLock = runtimeCache.getEvictionLock();
            cacheLock.lock();
            try {
                updateWeightLocked(newWeight);
            } finally {
                cacheLock.unlock();
            }
        }

        @GuardedBy("runtimeCache.getEvictionLock()")
        void updateWeightLocked(int newWeight) {
            int oldWeight = this.weight;
            if (oldWeight == newWeight) {
                return;
            }
            this.weight = newWeight;
            if (!runtimeCache.replaceQuietly(modelId, this, this)) {
                this.weight = oldWeight;
            }
        }

        final int loaderPredictedWeight() {
            try {
                return loader().predictSize(modelId, modelInfo);
            } catch (Exception e) {
                logger.error("ModelLoader for type " + modelInfo.getServiceType() + " failed to predict size for model "
                             + modelId + "; reverting to default: " + io.grpc.Status.fromThrowable(e));
                return defaultModelSizeUnits;
            }
        }

        // ------------- task prioritization logic --------------------

        // The CE is a Runnable in the loadingQueue, which
        // prioritizes based on the comparator ordering below

        // priority in queue for when loadings are backed up
        // only modified within lock of this CE object
        private long priority;

        /**
         * increase the priority of loading this model if it is
         * below the specified value and hasn't yet started/completed
         */
        public final void upgradePriority(long ifLowerThan, long to) {
            if (priority >= ifLowerThan || state > QUEUED) {
                return;
            }
            synchronized (this) {
                int st = state;
                if (priority >= ifLowerThan || st > QUEUED) {
                    return;
                }
                priority = to;
                // reposition in queue after increasing priority
                if (st == QUEUED && loadingQueue.remove(this)) {
                    loadingQueue.offer(this);
                }
            }
            logger.info("Queue priority of pending load of model " + modelId + " upgraded");
        }

        @Override
        public final int compareTo(CacheEntry<?> otherEntry) {
            // larger priority can overtake smaller priority
            return Long.compare(otherEntry.priority, priority);
        }

        // --------------------- state management -------------------------------

        private static final int
                // future  | possible next states
                //---------------------------------
                NEW = 0,     // not-set | -load->     QUEUED
                QUEUED = 1,  // not-set | -run->      WAITING or LOADING, -too-big-> FAILED
                WAITING = 2, // not-set | -wait->     LOADING
                LOADING = 3, // not-set | -complete-> ( FAILED or SIZING or ACTIVE ), -timeout-> FAILED
                SIZING = 4,  // set-ok  | -->         ACTIVE
                ACTIVE = 5,  // set-ok
                FAILED = 6,  // set-exception
                REMOVED = 7; // set (prior value OR ModelNotHereException)

        // also (anystate) -remove-> REMOVED

        private volatile int state = NEW;

        final String stateString() {
            return ModelMesh.ceStateString(state);
        }

        // this is updated prior to attempts to deregister+unload the
        // model from this instance, and is used only in the decision
        // of what to do with "orphan" cache entries (i.e. whether to
        // remove them from the cache or to update the registry)
        private volatile long lastUnloadAttemptTime = -1L;

        final boolean unloadAttemptedRecently() {
            return age(lastUnloadAttemptTime) < 600_000L; // 10 mins
        }

        public final boolean remove() {
            return remove(false);
        }

        /**
         * Abort loading. The entry will also be removed from the cache
         *
         * @param alreadyUnloaded true if already unloaded (e.g. runtime reported not found)
         */
        public final boolean remove(final boolean alreadyUnloaded) {
            if (state >= REMOVED) {
                return false;
            }

            // return false immediately if not evicted and no longer in cache; removal processing
            // will be performed by another thread that did the cache removal or eviction
            if (unloadManager == null) {
                return runtimeCache.remove(modelId, this) && doRemove(false, -1, alreadyUnloaded);
            }
            int removalWeight = unloadManager.removeEntry(this);
            return removalWeight != -1 && doRemove(false, removalWeight, alreadyUnloaded);
        }

        /**
         * Called after an entry is removed or evicted. unloadManager.entryRemoved or removeEntry must have
         * first been called.
         *
         * @param evicted  if already evicted from cache
         * @param removalWeight weight upon removal, only required if unloadManager != null
         * @param alreadyUnloaded true if already unloaded (e.g. runtime reported not found)
         * @return
         */
        final synchronized boolean doRemove(final boolean evicted,
                                            final int removalWeight,
                                            final boolean alreadyUnloaded) {
            int stateNow = state;
            if (stateNow >= REMOVED) {
                return false;
            }

            // important that this state change is prior to the cancel() below
            // since it may re-enter the lock in the complete() method
            state = REMOVED;

            if (stateNow == NEW) {
                // wake up waitWhileNew() method
                notifyAll();
            }

            long unloadDelayMs = -1L;
            if (stateNow <= LOADING) {
                // model is not yet loaded, no need to unload

                // abort - notify waiters
                setException(new ModelNotHereException(instanceId, modelId));

                if (stateNow == QUEUED) {
                    loadingCount.decrementAndGet();
                } else if (stateNow == LOADING || stateNow == WAITING) {
                    // cancel the in-progress loading
                    ListenableFuture<Void> lfut = loadFuture;
                    if (lfut != null) {
                        logger.info("Cancelling in-progress loading of model "
                                + modelId + " due to removal (state was " + ModelMesh.ceStateString(stateNow) + ")");
                        lfut.cancel(true);
                    }
                }
                if (unloadManager != null) {
                    if (stateNow == NEW || stateNow == QUEUED || stateNow == WAITING) {
                        // no more unload accounting to perform (already reverted by unloadManager.entryRemoved)
                        return true;
                    }
                    if (stateNow == LOADING) {
                        // this delay is to add a buffer between
                        // cancelling the in-progress load and
                        // initiating the subsequent unload
                        unloadDelayMs = evicted ? 500L : 3000L;
                    }
                }
            } else if (stateNow <= ACTIVE && !alreadyUnloaded) {
                // state is SIZING or ACTIVE
                if (stateNow == SIZING) {
                    // cancel the in-progress sizing
                    ListenableFuture<Void> lfut = loadFuture;
                    if (lfut != null && lfut.cancel(true)) {
                        logger.info("Cancelled in-progress sizing of model "
                                + modelId + " due to removal (state was " + SIZING + ")");
                    }
                }

                if (unloadManager != null) {
                    // Wait 200ms prior to initial check, to avoid race with incoming requests hitting.
                    // This still constitutes a race but the alternative is additional volatile checks
                    // on the 99.99% runtime path which we prefer to avoid.
                    // Give a bit more time in the sizing case to allow for the sizing request
                    // to be cancelled.
                    unloadDelayMs = stateNow == SIZING ? 1500L : 200L;
                } else {
                    // We treat removal of active model from the cache as our
                    // "unload" event if explicit unloading isn't enabled.
                    // Otherwise, this gets recorded in a callback set in the
                    // CacheEntry.unload(int) method
                    metrics.logTimingMetricDuration(Metric.UNLOAD_MODEL_TIME, 0L, false, modelId);
                    metrics.logCounterMetric(Metric.UNLOAD_MODEL);
                }
            }

            if (unloadManager != null) {
                if (unloadDelayMs < 0L) {
                    // Unload is already done or not applicable
                    unloadManager.unloadComplete(removalWeight, true, modelId);
                } else if (!shuttingDown) {
                    // here state == LOADING or ACTIVE
                    triggerUnload(removalWeight, unloadDelayMs);
                }
            }

            return true;
        }

        /**
         * @param weight         known loaded weight of the model to unload
         * @param initialDelayMs time to delay unloading initially,
         *                       after which further delay will depend on whether there
         *                       are any remaining inflight requests for the model
         */
        private void triggerUnload(int weight, long initialDelayMs) {
            if (unloadManager == null) {
                return;
            }
            long startNanos = nanoTime();
            if (initialDelayMs == 0L) {
                unloadIfIdle(weight, 1, startNanos);
            } else {
                if (initialDelayMs > 200L) {
                    logger.info("Delaying unload of model " + modelId + " for at least " + initialDelayMs + "ms");
                }
                taskPool.schedule(() -> unloadIfIdle(weight, 1, startNanos), initialDelayMs, MILLISECONDS);
            }
        }

        // Exponentially back off checks for remaining inflight requests; cap at 8 attempts i.e. ~128 seconds
        private void unloadIfIdle(int weight, int attempt, long startNanos) {
            if (!inflightRequests()) {
                if (attempt > 1) {
                    logger.info("Proceeding with unload after completion of inflight requests for model " + modelId
                                + ", waited " + (nanoTime() - startNanos) / M + "ms");
                }
                unload(weight);
            } else if (attempt >= 8) {
                logger.warn("Timed out waiting for inflight requests to complete after " + (nanoTime() - startNanos) / M
                            + " for model " + modelId + ", proceeding with unload");
                unload(weight);
            } else {
                if (attempt == 1) {
                    logger.info("Delaying unload of model " + modelId + " while there are requests in flight");
                }
                taskPool.schedule(() -> unloadIfIdle(weight, attempt + 1, startNanos), 500L << attempt, MILLISECONDS);
            }
        }

        private void unload(int weight) {
            logger.info("Initiating unload of model " + modelId); //TODO maybe check here whether shutting down
            long beforeNanos = nanoTime();
            Futures.addCallback(loader.unloadModel(modelId), new FutureCallback<>() {
                @Override
                public void onSuccess(Boolean reallyHappened) {
                    unloadManager.unloadComplete(weight, true, modelId);
                    // This should never be null, but guarding just in case
                    if (reallyHappened == null || reallyHappened) {
                        //TODO probably only log if took longer than a certain time
                        long tookMillis = msSince(beforeNanos);
                        logger.info("Unload of " + modelId + " completed in " + tookMillis + "ms");
                        metrics.logTimingMetricDuration(Metric.UNLOAD_MODEL_TIME, tookMillis, false, modelId);
                        metrics.logCounterMetric(Metric.UNLOAD_MODEL);
                    }
                    // else considered trivially succeeded because the corresponding
                    // prior load wasn't attempted, so don't log
                }

                @Override
                public void onFailure(Throwable throwable) {
                    if (shuttingDown) {
                        return; // ignore if shutting down
                    }
                    String type = modelInfo != null ? modelInfo.getServiceType() : "N/A";
                    logger.error("Unload failed for model " + modelId + " type=" + type
                                 + " after " + msSince(beforeNanos) + "ms", throwable);
                    unloadManager.unloadComplete(weight, false, modelId);
                    metrics.logCounterMetric(Metric.UNLOAD_MODEL_FAILURE);
                }
            }, directExecutor());
        }

        final void setLoadingQueueStartTime() {
            // lastHeavyTime field is used to store loading queue start time
            // when cache entry is in QUEUED state only
            assert state == QUEUED;
            lastHeavyTime = nanoTime();
        }

        final long getAndResetLoadingQueueStartTimeNanos() {
            // lastHeavyTime field is used to store loading queue start time
            // when cache entry is in QUEUED state only
            assert state == QUEUED;
            long startTime = lastHeavyTime;
            lastHeavyTime = 0L;
            return startTime;
        }

        /**
         * trigger asynchronous loading of the model
         */
        public synchronized boolean load(final int initialWeight, long lastUsed) {
            if (state != NEW) {
                if (state == REMOVED) {
                    return false;
                }
                logger.warn("Unexpected pre-loading state " + stateString() + " encountered for model " + modelId);
                return true;
            }
            if (shuttingDown) {
                remove();
                return false;
            }

            // inflate from INSERTION_WEIGHT to initial (predicted size)
            final int absWeight = Math.abs(initialWeight);
            if (unloadManager != null) {
                int increase = absWeight - getWeight();
                unloadManager.adjustNewEntrySpaceRequest(increase, this, initialWeight < 0);
            } else {
                updateWeight(initialWeight);
            }
            // check whether we were evicted when growing
            if (runtimeCache.getQuietly(modelId) != this) {
                logCacheFallthru(modelId, lastUsed, currentTimeMillis(), runtimeCache.oldestTime(), absWeight);
                return false;
            }

            state = QUEUED;
            loadingCount.incrementAndGet();
            notifyAll(); // wake up waitWhileNew() method

            logger.info("About to enqueue load for model " + modelId + " with initial weight " + absWeight + " units (~"
                        + mb(absWeight * UNIT_SIZE) + "), with priority " + priority);
            setLoadingQueueStartTime();
            try {
                loadingPool.execute(this);
            } catch (RejectedExecutionException ree) {
                if (shuttingDown) {
                    remove();
                    return false;
                }
                throw ree;
            }
            return true;
        }

        public final void waitWhileNew(long timeoutMs) throws InterruptedException {
            if (state != NEW || timeoutMs <= 0L) {
                return;
            }
            long deadline = currentTimeMillis() + timeoutMs;
            synchronized (this) {
                while (state == NEW && timeoutMs > 0L) {
                    wait(timeoutMs);
                    timeoutMs = deadline - currentTimeMillis();
                }
            }
        }

        // This future is used to cancel (interrupt) loading or sizing
        // - it covers the future task in which both of these are done
        private volatile ListenableFuture<Void> loadFuture;

        @Override
        public final void run() {
            boolean decrementLoadingCount = false;
            try {
                int absWeight;
                Exception preSizingError = null;
                synchronized (CacheEntry.this) {
                    if (state >= WAITING) {
                        return;
                    }
                    if (shuttingDown) {
                        remove();
                        return;
                    }
                    decrementLoadingCount = true;
                    long queueStartTimeNanos = getAndResetLoadingQueueStartTimeNanos();
                    if (queueStartTimeNanos > 0) {
                        long queueDelayMillis = (nanoTime() - queueStartTimeNanos) / M;
                        metrics.logSizeEventMetric(Metric.LOAD_MODEL_QUEUE_DELAY, queueDelayMillis, modelId);
                        // Only log if the priority value is "in the future" which indicates
                        // that there is or were runtime requests waiting for this load.
                        // Otherwise we don't care about arbitrary delays here
                        if (queueDelayMillis > 3000L && priority > currentTimeMillis()) {
                            logger.info("Load of model " + modelId + " spent ~" + ((queueDelayMillis / 1000) + 1)
                                        + "sec in loading queue (loadThreads = " + loadingThreads + ")");
                        }
                    }

                    absWeight = getWeight();
                    if (weight < 0) {
                        // if weight was a prediction based on existing average,
                        // increase it prior to actually loading
                        int loaderPredictedWeight = loaderPredictedWeight();
                        int increase = loaderPredictedWeight - absWeight;
                        if (increase > 0) {
                            long capacity = runtimeCache.capacity();
                            if (loaderPredictedWeight > capacity) {
                                // pathological case: predicted size > total capacity
                                preSizingError = new ModelLoadException(
                                        "Predicted size of " + gb(loaderPredictedWeight * UNIT_SIZE)
                                        + " for model " + modelId + " is larger than total" + " effective capacity of "
                                        + gb(capacity * UNIT_SIZE), instanceId, 0L, null);
                            } else if (unloadManager != null) {
                                unloadManager.adjustNewEntrySpaceRequest(increase, this, false);
                                absWeight = loaderPredictedWeight;
                            } else {
                                updateWeight(absWeight = loaderPredictedWeight);
                            }
                        }
                    }
                    if (preSizingError != null) {
                        setFailed(preSizingError);
                    } else {
                        state = unloadManager == null ? LOADING : WAITING;
                    }
                }
                if (preSizingError != null) {
                    failed(preSizingError, -1L);
                    return;
                }
                int targetWeight = absWeight;
                ListenableFutureTask<Void> lfut = ListenableFutureTask.create(() -> {
                    Thread curThread = Thread.currentThread();
                    String threadNameBefore = curThread.getName();
                    curThread.setName("model-load-" + modelId);
                    AtomicReference<Thread> aLoadThread = new AtomicReference<>(curThread);
                    ListenableFuture<?> timeoutFut = null;
                    long start = -1L;
                    ModelLoader.LoadedRuntime<T> runtime = null;
                    Throwable failure = null;
                    final String modelType = modelInfo.getServiceType();
                    try {
                        if (unloadManager != null) {
                            // this block-waits until there is available cache space to
                            // expand into, and moves the state from WAITING to LOADING
                            waitForSpaceToLoad(targetWeight);
                        }

                        logger.info("Starting load for model " + modelId + " type=" + modelType);

                        start = nanoTime();
                        timeoutFut = scheduleTimeoutForLoad(aLoadThread);
                        // perform the load
                        runtime = loader().loadRuntime(modelId, modelInfo);
                        long tookMillis = msSince(start);
                        loadingTimeStats(modelType).recordTime(tookMillis);
                        logger.info("Load of model " + modelId + " type=" + modelType + " completed in " + tookMillis
                                    + "ms");
                        metrics.logTimingMetricDuration(Metric.LOAD_MODEL_TIME, tookMillis, false, modelId);
                        metrics.logCounterMetric(Metric.LOAD_MODEL);
                    } catch (Throwable t) {
                        loadFuture = null;
                        failure = t;
                        if (start > 0) {
                            long took = msSince(start);
                            if (isInterruption(t)) {
                                logger.info("Load interrupted for model " + modelId + " type=" + modelType + " after "
                                            + took + "ms");
                            } else {
                                logger.error("Load failed for model " + modelId + " type=" + modelType + " after "
                                             + took + "ms");
                                metrics.logCounterMetric(Metric.LOAD_MODEL_FAILURE);
                            }
                        }
                    } finally {
                        aLoadThread.set(null);
                        if (timeoutFut != null) {
                            timeoutFut.cancel(false);
                        }
                        // The completion processing logic is in here
                        complete(runtime, failure);
                        if (failure != null) {
                            loadFuture = null; // this is done up-front (above) in failure == null case
                        }
                        curThread.setName(threadNameBefore);
                    }
                }, null);
                loadFuture = lfut;
                decrementLoadingCount = false; // done in complete() method
                lfut.run();
            } finally {
                if (decrementLoadingCount) { // just incase
                    loadingCount.decrementAndGet();
                }
            }
        }

        private void waitForSpaceToLoad(final int required) throws Exception {
            //assert unloadManager != null;
            // here state == WAITING; we wait if necessary for cache space to become available
            //  -- specifically that we can add back our prior subtraction from the aggregate
            // unloading weight without causing any further cache eviction
            final long beforeWaitNanos = nanoTime(), deadlineNanos = beforeWaitNanos + MINUTES.toNanos(3L); //TODO TBC
            try {
                while (true) {
                    //TODO may need to change this to grab space incrementally, otherwise
                    // load of large model could be held up indefinitely by loads of smaller ones
                    boolean timeout = !unloadManager.waitForSpaceToLoad(required, () -> state != WAITING, deadlineNanos);
                    if (timeout) {
                        throw noStack(new TimeoutException("Model " + modelId + " spent > 3mins"
                                        + " pre-load waiting for space to become available"));
                    }
                    synchronized (CacheEntry.this) {
                        final int st = state;
                        if (st != WAITING) {
                            throw new Exception("State of model " + modelId + " changed to " + ModelMesh.ceStateString(st)
                                                + " during pre-load waiting, aborting load");
                        }
                        if (unloadManager.claimRequestedSpaceIfReady(required)) {
                            state = LOADING;
                            break;
                        }
                    }
                }
            } finally {
                long waitedTimeMillis = NANOSECONDS.toMillis(nanoTime() - beforeWaitNanos);
                if (waitedTimeMillis > 20) {
                    logger.info("Load of model " + modelId + " waited " + waitedTimeMillis
                                + "ms for cache space to become free");
                }
            }
        }

        // timeout starts from when the loading actually starts, not when it's queued
        private ListenableFuture<?> scheduleTimeoutForLoad(AtomicReference<Thread> loadThread) {
            return taskPool.schedule(() -> {
                // load timeout
                Throwable timeout = new TimeoutException(
                        "Load of model " + modelId + " timed out after " + (loadTimeoutMs / 1000L) + "sec");
                Thread t = loadThread.get();
                if (t != null) {
                    Throwable threadStack = new Throwable("Point-in-time stacktrace of timed-out model load:");
                    threadStack.setStackTrace(t.getStackTrace());
                    timeout.initCause(threadStack);
                }
                synchronized (CacheEntry.this) {
                    // assert state >= LOADING;
                    // note in particular we can't be in WAITING state yet since the
                    // timeout is only scheduled after we move from WAITING -> LOADING
                    if (state > LOADING) {
                        return;
                    }
                    if (!setFailed(timeout)) {
                        logger.warn("Cache entry future found already completed after timeout for model " + modelId);
                    }
                }
                Future<Void> fut = loadFuture;
                if (fut != null) {
                    fut.cancel(true); // cancel/interrupt loading task
                }
                failed(timeout, 3000L);
            }, loadTimeoutMs, MILLISECONDS);
        }

        /**
         * Called when *loading* completes
         */
        protected final void complete(LoadedRuntime<T> result, Throwable error) {
            // This is called after loading is complete (succeeded or failed)
            // but within the loading future task, to allow in-progress sizing
            // requests to be cancelled in the event of concurrent model removal
            try {
                int size = -1;
                synchronized (this) {
                    if (state > LOADING) {
                        return;
                    }

                    if (error == null && (result == null || result.getRuntime() == null)) {
                        error = new NullPointerException("Loader returned null runtime");
                    }
                    if (error != null) {
                        if (unloadManager != null && state == WAITING) {
                            if (!isDone()) {
                                // this won't leave a failure registration unlike loading failures
                                logger.warn("Model " + modelId + " pre-load aborted "
                                            + "while waiting for space to free up", error);
                            }
                            remove();
                            return;
                        }

                        if (!setFailed(error)) {
                            logger.warn("Cache entry future found already"
                                        + " completed after failure for model " + modelId);
                        }
                    } else {
                        afterLoad(result);
                        size = Math.max(result.getSizeInUnits(), 0);
                        state = size > 0 ? ACTIVE : SIZING;
                        loadCompleteTimestamp = currentTimeMillis();
                        if (!set(result.getRuntime())) {
                            logger.error("Cache entry future found already completed after load of model " + modelId);
                        }
                    }
                }

                if (size < 0) {
                    failed(error, 1000L);
                } else {
                    logger.info("Model " + modelId + " loading completed successfully"
                                + (limitModelConcurrency ? ", maxConcurrency = " + result.getMaxConcurrency() : ""));

                    processStartupFailFastSuccess();

                    if (size > 0) {
                        long sizeBytes = size * UNIT_SIZE;
                        logger.info("Model " + modelId + " size = " + size + " units" + ", ~" + mb(sizeBytes));
                        metrics.logSizeEventMetric(Metric.LOADED_MODEL_SIZE, sizeBytes, modelId);
                    } else {
                        try {
                            long before = nanoTime();
                            size = loader().modelSize(result.getRuntime());
                            if (state <= ACTIVE) {
                                long took = msSince(before), sizeBytes = size * UNIT_SIZE;
                                logger.info("Model " + modelId + " size = " + size + " units" + ", ~" + mb(sizeBytes)
                                            + " sizing took " + took + "ms");
                                metrics.logTimingMetricDuration(Metric.MODEL_SIZING_TIME, took, false, modelId);
                                // this is actually a size (bytes), not a "time"
                                metrics.logSizeEventMetric(Metric.LOADED_MODEL_SIZE, sizeBytes, modelId);
                            }
                        } catch (Exception e) {
                            if (!isInterruption(e) && state == SIZING) {
                                logger.error("Model size measurer failed for model " + modelId + " type "
                                             + modelInfo.getServiceType(), e);
                            }
                            // revert to predicted size
                            if (size == 0) {
                                size = getWeight();
                            }
                        }
                    }

                    synchronized (this) {
                        if (state == SIZING) state = ACTIVE;
                        else if (state != ACTIVE) return;

                        int weightBefore = getWeight(), sizedWeight = Math.max(size, 1);
                        int delta = sizedWeight - weightBefore;
                        if (delta != 0) {
                            if (unloadManager != null) {
                                unloadManager.adjustWeightAfterLoad(delta, this);
                            }
                            // update instance record (e.g. maybe went from not-full -> full)
                            publishInstanceRecordAsync();
                        }
                    }
                }
            } finally {
                loadingCount.decrementAndGet();
            }
        }

        // Called only from synchronized context
        private boolean setFailed(Throwable t) {
            state = FAILED;
            loadCompleteTimestamp = currentTimeMillis();
            return setException(t);
        }

        // Called at most once, by the thread which moved the state to FAILED.
        // unloadDelay == -1L means load wasn't attempted so don't unload at all
        private void failed(Throwable t, long unloadDelay) {
            if (state != FAILED) {
                return;
            }

            boolean isShuttingDown = shuttingDown;
            if (!isShuttingDown) {
                logger.error("Model load failed for " + modelId + " type=" + modelInfo.getServiceType(), t);
            }

            int weightBefore;
            synchronized (CacheEntry.this) {
                weightBefore = getWeight();
                int weightReduction = weightBefore - FAILED_WEIGHT;
                if (weightReduction != 0) {
                    if (unloadManager != null) {
                        // Though this method is written the new entry adjustment, the accounting
                        // is identical to what we need here when reducing the size of the failed
                        // entry and triggering an unload tied to that space reduction.
                        unloadManager.adjustNewEntrySpaceRequest(-weightReduction, this, false);
                        if (!isShuttingDown && unloadDelay >= 0) {
                            triggerUnload(weightReduction, unloadDelay);
                        } else {
                            unloadManager.unloadComplete(weightReduction, true, modelId);
                        }
                    } else {
                        updateWeight(FAILED_WEIGHT);
                    }
                }
            }

            try {
                long lastUsed = runtimeCache.getLastUsedTime(modelId);
                ModelRecord mr = registry.getOrStrongIfAbsent(modelId);
                while (true) {
                    if (mr == null) {
                        return; // deleted while loading
                    }
                    if (lastUsed <= 0L) {
                        lastUsed = mr.getLastUsed();
                    }
                    boolean wasThere = mr.getInstanceIds().remove(instanceId, loadTimestamp);
                    if (!wasThere) {
                        logger.warn("Instance record not found for model " + modelId + " after failure"); // shouldn't happen
                        break;
                    }
                    if (!shuttingDown) {
                        mr.addLoadFailure(instanceId, loadCompleteTimestamp, failureMessageFromException(t));
                    }
                    mr.updateLastUsed(lastUsed);
                    ModelRecord existMr = registry.conditionalSetAndGet(modelId, mr);
                    if (existMr == mr) {
                        logger.info("ModelRecord updated for " + modelId + ": locations=" + mr.getInstanceIds()
                                    + ", failures=" + mr.getLoadFailedInstanceIds());
                        break;
                    }
                    mr = existMr;
                }

                // Attempt to retry load on different instance.
                // the "cache miss" logic takes into account the count
                // and location of recent failures, so the number of
                // retries won't be excessive.

                // Note this won't actually retry if this failed load
                // was a scale-up rather than a first copy load. In the
                // scaleup case, if the conditions which triggered it
                // still hold then it should be re-triggered in due course
                // anyhow.

                if (lastUsed < 0L) lastUsed = 0L; // extra precaution
                ensureLoadedInternalAsync(modelId, lastUsed, weightBefore, excludeThisInstance, 0);

                processStartupFailFastFailure(mr);

            } catch (Exception e) {
                if (!isInterruption(e)) {
                    logger.error("Exception while updating record or initiating retry after failed load of model "
                                 + modelId, e);
                }
            }
        }

        // Used when there is a failure updating the KV registry prior to a load attempt
        final synchronized boolean preloadFailed(Throwable t) {
            if (state != NEW) {
                return false;
            }
            setFailed(t);
            notifyAll(); // wake up waitWhileNew() method
            return true;
        }

        public final boolean isFailed() {
            Throwable fe = finalException();
            return fe != null && !(fe instanceof ModelNotHereException);
        }

        public final boolean isFinished() {
            return state >= FAILED;
        }

        public final boolean isAborted() {
            return finalException() instanceof ModelNotHereException;
        }

        public final boolean isRemoved() {
            return state >= REMOVED;
        }

        final Throwable finalException() {
            if (isDone()) {
                try {
                    Uninterruptibles.getUninterruptibly(this);
                } catch (ExecutionException t) {
                    return t.getCause();
                }
            }
            return null;
        }

        @SuppressWarnings("unchecked")
        private ModelLoader<T> loader() {
            return (ModelLoader<T>) loader;
        }

        protected void afterLoad(LoadedRuntime<T> runtime) {
            if (runtime.getMaxConcurrency() > 0 && !limitModelConcurrency) {
                logger.warn("Ignoring max concurrency of " + runtime.getMaxConcurrency() + " returned by loaded model "
                            + modelId + " since we are not in limitConcurrency mode");
            }
        }

        protected void beforeInvoke(int requestWeight)
                throws InterruptedException, ApplierException, ModelNotHereException { //TODO exception types?
            recordInvocation(requestWeight); // model-level invoke counter
            invokeCounter.recordHits(requestWeight); // instance-level invoke counter
        }

        protected void afterInvoke(int requestWeight, long tookNanos) {
            if (invokeCompletionCount != null) {
                invokeCompletionCount.add(requestWeight); // model-level invoke
            }
        }
    }

    static String failureMessageFromException(Throwable t) {
        if (t instanceof ExecutionException || t instanceof UncheckedExecutionException) {
            t = t.getCause();
        }
        // Don't include generic exception names in the message
        if (t.getClass() == Exception.class) {
            return t.getMessage();
        }
        io.grpc.Status status;
        if (t instanceof StatusRuntimeException) {
            status = ((StatusRuntimeException) t).getStatus();
        } else if (t instanceof StatusException) {
            status = ((StatusException) t).getStatus();
        } else {
            return String.valueOf(t);
        }
        String description = status.getDescription();
        if (description == null) {
            return status.getCode().toString();
        }
        // Don't include "UNKNOWN:" string if there is a corresponding message
        return status.getCode() == Code.UNKNOWN ? description
                : status.getCode() + ": " + description;
    }

    static final StackTraceElement[] EMPTY_STACK = new StackTraceElement[0];

    static <T extends Throwable> T noStack(T exception) {
        exception.setStackTrace(EMPTY_STACK);
        return exception;
    }

    static final String[] CE_STATE_STRS = {
            "NEW", "QUEUED", "WAITING", "LOADING", "SIZING", "ACTIVE", "FAILED", "REMOVED"
    };

    static final String RESOURCE_EXHAUSTED = Code.RESOURCE_EXHAUSTED.name();

    static final ApplierException QUEUE_BREACH_EXCEPTION = noStack(
            new ApplierException("Model queue overload", null, RESOURCE_EXHAUSTED));

    static boolean isExhausted(Throwable e) {
        return e instanceof ApplierException && RESOURCE_EXHAUSTED.equals(((ApplierException) e).getGrpcStatusCode());
    }

    /**
     * This subclass is used IFF we are running in model-concurrency-limiting mode
     * ({@link #limitModelConcurrency} == true)
     */
    final class MaxConcCacheEntry<T> extends CacheEntry<T> {

        final long timeoutMicros = 0; //TODO

        /*final*/ int maxConc;

        //TODO also think about accommodation of scale-up spikes (temporarily can be longer)
        // kind of linked to the 90% below also
        /*final*/ Semaphore allWaiting;

        /*final*/ Semaphore limiter;

        private static final int COUNT_BITS = 21;

        // Rightmost COUNT_BITS bits hold count of completed invocations,
        // other (63-COUNT_BITS) bits hold sum of times of those invocations
        // in 1/10th of millisecs. Relies on count never reaching 2^COUNT_BITS
        // within rate-tracking window (default 11sec)
        private final LongAdder countAndTimeSum = new LongAdder();

        public MaxConcCacheEntry(String modelId, ModelRecord mr, long loadTimestamp, long priority) {
            super(modelId, mr, loadTimestamp, priority);
        }

        @Override
        protected boolean recordInvokeCompletions() {
            // inflight requests are tracked via the semaphores, not the adders
            return false;
        }

        @Override
        protected void afterLoad(LoadedRuntime<T> runtime) {
            int maxConc = runtime.getMaxConcurrency();
            if (maxConc <= 0) {
                logger.warn("Max concurrency of " + maxConc + " returned for loaded model " + modelId
                            + ", defaulting to 1");
                maxConc = 1;
            }
            this.maxConc = maxConc;
            limiter = new Semaphore(maxConc, true);
            if (maxInflightPerModelCopy >= 0) {
                int maxWaiting = maxInflightPerModelCopy > 0 ? maxInflightPerModelCopy : Math.max(16, maxConc * 8);
                allWaiting = new Semaphore(maxWaiting);
            }
            // else no limit
        }

        @Override
        protected void beforeInvoke(int requestWeight)
                throws InterruptedException, ApplierException, ModelNotHereException {
            recordInvocation(requestWeight);
            boolean release = false, success = false;
            long beforeNanos = 0L;
            try {
                if (allWaiting != null) {
                    if (allWaiting.tryAcquire()) {
                        release = true;
                    } else {
                        throw QUEUE_BREACH_EXCEPTION;
                    }
                }
                beforeNanos = nanoTime();
                //TODO more permits in batch case (when added)??
                if (timeoutMicros != 0L) {
                    if (!limiter.tryAcquire(timeoutMicros, MICROSECONDS)) {
                        throw new RuntimeException("queue timeout");
                    }
                } else {
                    limiter.acquire();
                }
                release = false;
                success = true;
            } finally {
                if (release) {
                    allWaiting.release();
                }
                if (beforeNanos != 0L) {
                    long tookMillis = msSince(beforeNanos);
                    // If we waited for more than 100ms in the queue, check that
                    // the model wasn't since removed (may be waiting to unload)
                    if (success && tookMillis > 100L && isRemoved()) {
                        limiter.release();
                        //noinspection ThrowFromFinallyBlock
                        throw new ModelNotHereException(instanceId, modelId);
                    }
                    metrics.logTimingMetricDuration(Metric.QUEUE_DELAY, tookMillis, false, modelId);
                }
            }
        }

        @Override
        protected void afterInvoke(int requestWeight, long tookNanos) {
            limiter.release();
            if (allWaiting != null) {
                allWaiting.release();
            }
            invokeCounter.recordTimeNanos(requestWeight * tookNanos); // instance-level invoke counter
            countAndTimeSum.add(1L + ((tookNanos / 100_000L) << COUNT_BITS));
        }

        boolean hasQueuedRequests() {
            Semaphore sem = limiter;
            return sem != null && sem.hasQueuedThreads();
        }

        int queuedRequestCount() {
            Semaphore sem = limiter;
            return sem != null ? sem.getQueueLength() : 0;
        }

        @Override
        boolean inflightRequests() {
            Semaphore sem = limiter;
            return sem != null && sem.availablePermits() < maxConc;
        }

        //TODO think about way of including in-flight elapsed times which are > the-average
        // but without double-counting

        static final long COUNT_MASK = (1 << COUNT_BITS) - 1;

        // these modified only by ratetracker task
        volatile int priorCount;
        volatile long priorSum; //maybe combine

        int getRpmScaleThreshold(boolean andReset) {
            long curVal = countAndTimeSum.sum(), timeSum;
            int count = (int) (curVal & COUNT_MASK);
            if (count >= 64) {
                if (andReset) {
                    curVal = countAndTimeSum.sumThenReset();
                    priorSum = timeSum = curVal >>> COUNT_BITS;
                    priorCount = count = (int) (curVal & COUNT_MASK);
                } else {
                    timeSum = curVal >>> COUNT_BITS;
                }
            } else {
                // supplement with prior count
                int pc = priorCount;
                if (pc <= 0 && count < 8) {
                    return scaleUpRpmThreshold; //TODO tbd
                }
                timeSum = priorSum + (count > 0 ? (curVal >>> COUNT_BITS) : 0L);
                count += pc;
            }

            if (timeSum == 0L) {
                return Integer.MAX_VALUE;
            }

            //TODO take into account current number of queued requests

            // percentage of of estimated bandwidth (default 90%)
            // ( dynamicRpmScaleConstant == percentage * 600_000 (1 min in 1/10th millis) )
            return (int) ((maxConc * (count * dynamicRpmScaleConstant)) / timeSum);
        }
    }

    /* ----------------------------- cache and model registry events --------------------------------------------- */

    boolean initialized;

    /*
     * Events produced by the kv-store (zookeeper/etcd) model registry
     */
    @Override
    public void event(EventType type, String key, ModelRecord record) {
        if (type == INITIALIZED) {
            loadedModelCount = loadedModelIds.size();
            failedModelCount = failedModelIds.size();
            initialized = true;
            return;
        }
        if (type == ENTRY_DELETED && hasRegistration(record, instanceId)) {
            try {
                logger.info("Removing cache entry for model deleted from registry: " + key + ", record was: " + record);
                //TODO or maybe remove first -- but think about locking implications
                CacheEntry<?> ce = runtimeCache.getQuietly(key);
                if (ce != null) {
                    ce.remove();
                }
            } catch (Exception e) {
                logger.warn("Exception processing model deletion event for: " + key, e);
            }
        }

        // update maps of global loaded/failed model counts, publish as metrics if we're leader
        boolean loaded = type != ENTRY_DELETED && !record.getInstanceIds().isEmpty();
        boolean failed = type != ENTRY_DELETED && record.hasLoadFailure();
        boolean loadedChanged = loaded ? loadedModelIds.add(key) : loadedModelIds.remove(key);
        boolean failedChanged = failed ? failedModelIds.add(key) : failedModelIds.remove(key);

        if (!initialized) {
            return;
        }

        boolean isLeader = isLeader();

        if (vModelManager != null && isLeader && (record == null || record.getRefCount() > 0)) {
            // Check whether this model is a "blocking" vmodel target
            vModelManager.processModelChange(key);
        }

        if (loadedChanged) loadedModelCount = loadedModelIds.size();
        if (failedChanged) failedModelCount = failedModelIds.size();
        // publish model stats if leader
        if (metricsSetup && metrics.isEnabled() && isLeader) {
            if (loadedChanged) metrics.logGaugeMetric(Metric.MODELS_LOADED, loadedModelCount);
            if (failedChanged) metrics.logGaugeMetric(Metric.MODELS_WITH_LOAD_FAIL, failedModelCount);
            if (type != ENTRY_UPDATED) {
                metrics.logGaugeMetric(Metric.TOTAL_MODELS, registry.getCount());
            }
        }
    }

    private static boolean hasRegistration(ModelRecord record, String instance) {
        return record.getInstanceIds().containsKey(instance) || record.loadFailedInInstance(instance);
    }

    /*
     * Event produced by the local model cache
     */
    @Override
    public void onEviction(String key, CacheEntry<?> value) {}

    @Override
    public void onEviction(String key, CacheEntry<?> ce, long lastUsed) {
        long now = currentTimeMillis();

        // Log prior to dispatch so we can see thread which triggered the eviction
        int removalWeight = ce.getWeight();
        logger.info("Eviction triggered for model " + key
            + " (weight=" + removalWeight + " / " + mb(removalWeight * UNIT_SIZE) + ")");

        // we hold the eviction lock here
        if (unloadManager != null) {
            unloadManager.entryRemoved(removalWeight);
        }

        // perform deregistration and removal re-attempt on regular task thread; eviction
        // callbacks are now performed under the cache lock
        taskPool.execute(() -> {
            // determine whether a reload should be attempted elsewhere:
            // *don't* reload if this was a cached load failure, or was
            // only just loaded (e.g. an immediate eviction)
            boolean attemptReload = false, inRegistry = false, failed = ce.isFailed();
            if (!failed) {
                ModelRecord mr = registry.get(key);
                if (mr != null) {
                    Long loadedTime = mr.getInstanceIds().get(instanceId);
                    if (loadedTime == null) {
                        loadedTime = mr.getLoadFailedInstanceIds().get(instanceId);
                    }
                    inRegistry = loadedTime != null;
                    attemptReload = inRegistry && (now - loadedTime) > 2 * loadTimeoutMs;
                }
            }

            long millisSinceLastUsed = now - lastUsed;
            if (inRegistry || failed) {
                // don't record "immediate" evictions (additions too old to fit in cache at all)
                logger.info("Evicted " + (failed ? "failed model record" : "model") + " " + key
                    + " from local cache, last used " + readableTime(millisSinceLastUsed) + " ago (" + lastUsed
                    + "ms), invoked " + ce.getTotalInvocationCount() + " times");
                metrics.logTimingMetricDuration(Metric.AGE_AT_EVICTION, millisSinceLastUsed, false, ce.modelId);
                metrics.logCounterMetric(Metric.EVICT_MODEL);
            }

            // abort/unload model
            ce.doRemove(true, removalWeight, false);

            // remove our record from registry
            deregisterModel(key, lastUsed, ce.loadTimestamp, ce.loadCompleteTimestamp);

            if (attemptReload) {
                // if the cluster is less than 95% full, attempt to re-load this model elsewhere
                // (rebalancing)
                ClusterStats stats = typeSetStats(ce.modelInfo.getServiceType());
                if (stats.totalCapacity > 0 && stats.instanceCount > 1
                    && ((20L * stats.totalFree) / stats.totalCapacity) >= 1) {
                    try {
                        StatusInfo si = ensureLoadedElsewhere(key, lastUsed, ce.getWeight());
                        if (si.getStatus() == Status.LOADING) {
                            logger.info("Triggered load of evicted model "
                                + key + " elsewhere (cluster isn't full)");
                        }
                    } catch (Exception e) {
                        logger.warn("Exception attempting to load evicted model " + key + " elsewhere");
                    }
                }
            }
        });
    }

    /**
     * @param modelId
     * @param lastUsed
     * @param loadTime if non-null, only records with this loadTime will be removed
     */
    protected void deregisterModel(final String modelId, final long lastUsed,
            final Long loadTime, long loadCompletedTime) {
        try {
            // remove from the registry
            ModelRecord mr = registry.getOrStrongIfAbsent(modelId);
            if (mr == null) {
                return;
            }
            do {
                // shouldn't be in both maps, but removing from both just in case
                Map<String, Long> iids = mr.getInstanceIds();
                boolean wasThere = loadTime != null ?
                        iids.remove(instanceId, loadTime) : (iids.remove(instanceId) != null);
                boolean failedWasThere = loadTime != null ?
                        mr.removeLoadFailure(instanceId, loadCompletedTime) : mr.removeLoadFailure(instanceId);
                if (!wasThere && !failedWasThere) return;
                mr.updateLastUsed(lastUsed);
                if (wasThere) mr.updateLastUnloadTime();
                ModelRecord existMr = registry.conditionalSetAndGet(modelId, mr);
                if (existMr == mr || existMr == null) return;
                mr = existMr;
            } while (true);
        } catch (Exception e) {
            logger.warn("Exception during registry-removal for model " + modelId, e);
        }
    }

    /**
     * @param modelId
     * @param lastUsed
     * @param loadTime if non-null, only records with this loadTime will be removed
     */
    protected void deregisterModelAsync(final String modelId, final long lastUsed,
            final Long loadTime, final long loadCompletedTime) {
        taskPool.execute(() -> deregisterModel(modelId, lastUsed, loadTime, loadCompletedTime));
    }

    /* --------------------------------- StatusInfo constants ---------------------------------------------------- */

    public static final class ExtendedStatusInfo extends StatusInfo {
        public static final class CopyInfo implements Comparable<CopyInfo> {
            static final CopyInfo[] EMPTY = new CopyInfo[0];
            final String location;
            final Status status;
            final long time;

            public CopyInfo(String location, Status status, long time) {
                this.location = location;
                this.status = status;
                this.time = time;
            }

            @Override
            public int compareTo(CopyInfo o) {
                return Long.compare(o.time, time);
            }
        }

        final CopyInfo[] copiesInfo;

        ExtendedStatusInfo(Status status, List<String> messages, CopyInfo[] copiesInfo) {
            super(status, messages);
            this.copiesInfo = copiesInfo;
        }
    }

    protected static final StatusInfo SI_LOADED = new StatusInfo(Status.LOADED, null);
    protected static final StatusInfo SI_LOADING = new StatusInfo(Status.LOADING, null);
    protected static final StatusInfo SI_NOT_LOADED = new StatusInfo(Status.NOT_LOADED, null);
    protected static final StatusInfo SI_LOADING_FAILED = new StatusInfo(Status.LOADING_FAILED, null);
    protected static final StatusInfo SI_NOT_FOUND = new StatusInfo(Status.NOT_FOUND, null);

    // Fill-in the model-copy information in the returned StatusInfo
    protected static StatusInfo makeStatusInfo(Status status, ModelLoadException mle, ModelRecord mr) {
        Map<String, Long> failTimes = mr != null ? mr.getLoadFailedInstanceIds() : Collections.emptyMap();
        Map<String, FailureInfo> failInfos = mr != null ? mr.getLoadFailureInfos() : Collections.emptyMap();
        Map<String, Long> instTimes = mr != null ? mr.getInstanceIds() : Collections.emptyMap();
        if (mle != null && mle.getInstanceId() != null && !failTimes.containsKey(mle.getInstanceId())) {
            failTimes = new TreeMap<>(failTimes);
            failInfos = new TreeMap<>(failInfos);
            failTimes.put(mle.getInstanceId(), currentTimeMillis());
            failInfos.put(mle.getInstanceId(), new FailureInfo(mle.getMessage()));
            if (instTimes.containsKey(mle.getInstanceId())) {
                instTimes = new TreeMap<>(instTimes);
                instTimes.remove(mle.getInstanceId());
            }
        }
        List<CopyInfo> copiesInfo = null;
        List<String> messages = null;
        if (!instTimes.isEmpty()) {
            // We can't yet distinguish precisely between LOADED and LOADING so flagging all of these as
            // UNKNOWN. This will change with load events updates that are planned to be added soon.
            copiesInfo = new ArrayList<>(instTimes.size());
            for (Entry<String, Long> ent : instTimes.entrySet()) {
                copiesInfo.add(new CopyInfo(ent.getKey(), Status.NOT_CHECKED, ent.getValue()));
            }
        }
        if (!failTimes.isEmpty()) {
            final Map<String, Long> fft = failTimes;
            if (copiesInfo == null) {
                copiesInfo = new ArrayList<>(4);
            }
            // Sort message list by time of failure (most recent first)
            messages = failInfos.entrySet().stream()
                    .sorted((e1, e2) -> fft.get(e2.getKey()).compareTo(fft.get(e1.getKey())))
                    .map(e -> e.getValue().getMessage()).collect(Collectors.toList());
            for (Entry<String, Long> ent : failTimes.entrySet()) {
                copiesInfo.add(new CopyInfo(ent.getKey(), Status.LOADING_FAILED, ent.getValue()));
            }
        } else if (mle != null && !Strings.isNullOrEmpty(mle.getMessage())) {
            messages = Collections.singletonList(mle.getMessage());
        }
        if (copiesInfo == null) {
            return new StatusInfo(status, messages);
        }
        // Ensure sorted in reverse chronological order
        Collections.sort(copiesInfo);
        return new ExtendedStatusInfo(status, messages, copiesInfo.toArray(CopyInfo.EMPTY));
    }

    /* --------------------------------- public API lifecycle methods -------------------------------------------- */

    //TODO need to be careful about expected state on various exceptions (add, load may/may not have happened)

    /**
     * Adds a trained model to this model-mesh cluster.
     *
     * @param modelId   the id of the model to add
     * @param modelInfo information required to load/serve the model, including the type
     * @param loadNow   whether the model should be loaded immediately
     * @param sync      if load is true, whether this method should block until the load completes
     */
    @Idempotent
//  @Override
    public StatusInfo addModel(String modelId, ModelInfo modelInfo, boolean loadNow, boolean sync)
            throws InvalidInputException, InternalException {
        return registerModel(modelId, modelInfo, loadNow, sync, 0L);
    }

    /**
     * Adds a trained model to this model-mesh cluster.
     *
     * @param modelId               the id of the model to add
     * @param modelInfo             information required to load/serve the model, including the type
     * @param loadNow               whether the model should be loaded immediately
     * @param sync                  if load is true, whether this method should block until the load completes
     * @param initialCacheTimestamp initial cache timestamp; 0 means "not specified, use default"
     */
    protected StatusInfo registerModel(String modelId, ModelInfo modelInfo, boolean loadNow, boolean sync,
            long initialCacheTimestamp) throws InvalidInputException, InternalException {
        if (Strings.isNullOrEmpty(modelId) || modelInfo == null) {
            throw new InvalidInputException("Must provide non-empty modelId and modelInfo");
        }
        String type = validateNewModelType(modelInfo.getServiceType());
        if ("".equals(modelInfo.getEncKey())) modelInfo.setEncKey(null);
        if ("".equals(modelInfo.getModelPath())) modelInfo.setModelPath(null);

        final long now = currentTimeMillis();
        final long lastUsedTimestamp = initialCacheTimestamp >= now ? now
                : (initialCacheTimestamp > 0 ? initialCacheTimestamp
                    // set record lastUsed age to 6x older if we're not loading now
                    : now - (LASTUSED_AGE_ON_ADD_MS * (loadNow ? 1L : 6L)));
        boolean weCreated;
        try {
            ModelRecord mr = registry.get(modelId);
            // if already exists, ensure we have the latest
            if (mr != null) {
                mr = registry.getConsistent(modelId);
            }
            while (true) {
                if (mr != null) {
                    if (!mr.getType().equals(type)
                        || !Objects.equals(mr.getEncryptionKey(), modelInfo.getEncKey())
                        || !Objects.equals(mr.getModelPath(), modelInfo.getModelPath())) {
                        logger.warn("Attempt to add a model " + modelId + " of type " + type + " and path "
                                + modelInfo.getModelPath() + " that already exists with different attribute(s): type="
                                + mr.getType() + ", path=" + mr.getModelPath());
                        throw new InvalidInputException("Model already exists with different attribute values");
                    }
                    // model is already here, but it's the same
                    if (!loadNow && lastUsedTimestamp > (mr.getLastUsed() + LASTUSED_AGE_ON_ADD_MS)) {
                        // in loadNow case, this will be done via ensureLoaded below
                        mr.updateLastUsed(lastUsedTimestamp);
                        ModelRecord existMr = registry.conditionalSetAndGet(modelId, mr);
                        if (existMr != mr) {
                            mr = existMr;
                            continue;
                        }
                    }
                    weCreated = false;
                } else {
                    if (readOnlyMode) {
                        logger.warn("Rejecting registerModel for " + modelId + " while in read-only mode");
                        throw newInternalException("model-mesh read-only mode", null);
                    }
                    mr = new ModelRecord(modelInfo.getServiceType(), modelInfo.getEncKey(),
                            modelInfo.getModelPath(), false);
                    mr.setLastUsed(lastUsedTimestamp);
                    ModelRecord existMr = registry.conditionalSetAndGet(modelId, mr);
                    if (existMr != mr) {
                        mr = existMr;
                        continue;
                    }
                    logger.info("Added new model " + modelId + " to registry of type " + modelInfo.getServiceType());
                    weCreated = true;
                }
                break;
            }
        } catch (InvalidInputException iie) {
            throw iie;
        } catch (Exception e) {
            throw newInternalException("Failed to add model " + modelId, e);
        }
        StatusInfo status;
        if (loadNow) {
            try {
                //TODO(later) in load + doesn't already exist case, consider how to consolidate to single
                //  kv store write (i.e. first forwarding to next "cache miss" target
                status = ensureLoaded(modelId, lastUsedTimestamp, null, sync, true);
            } catch (ModelNotFoundException mle) {
                status = SI_NOT_FOUND; // this would be an unlikely occurrence
            }
        } else {
            status = weCreated ? SI_NOT_LOADED : getStatus(modelId);
        }
        if (status != null && status.getStatus() == Status.NOT_FOUND) {
            logger.warn("registerModel returning NOT_FOUND for model " + modelId
                        + " (must have been concurrently deleted)");
        }
        return status;
    }

    /**
     * Removes the model with the specified id from this model-mesh cluster.
     * Has no effect if the specified model isn't found. Will fail if the
     * model is required by any other existing models or vmodels.
     *
     * @param modelId
     */
    @Idempotent
//  @Override
    public void deleteModel(String modelId) throws InvalidStateException, InternalException {
        try {
            ModelRecord mr = registry.get(modelId);
            if (mr == null || mr.getRefCount() > 0 || readOnlyMode) {
                mr = registry.getStrong(modelId);
            }
            while (mr != null) { // fine if not found
                if (mr.getRefCount() > 0) {
                    logger.warn("Rejecting unregisterModel for modelId " + modelId + " because it has "
                                + mr.getRefCount() + " referent(s)");
                    throw new InvalidStateException("Model " + modelId + " has referents");
                }
                if (readOnlyMode) {
                    logger.warn("Rejecting unregisterModel for modelId " + modelId + " while in read-only mode");
                    throw newInternalException("model-mesh read-only mode", null);
                }

                if (registry.conditionalDelete(modelId, mr)) { // successful delete
                    logger.info("Model " + modelId + " deleted");
                    return;
                }
                mr = registry.getConsistent(modelId);
            }
        } catch (Exception e) {
            Throwables.throwIfInstanceOf(e, InvalidStateException.class);
            Throwables.throwIfInstanceOf(e, InternalException.class);
            throw newInternalException("Unexpected error deleting model " + modelId, e);
        }
    }

    /**
     * Ensures the model with the specified id is loaded in this model-mesh cluster.
     *
     * @param modelId
     * @param lastUsed         the timestamp associated with the load, or 0 for "now" (most typical)
     * @param excludeInstances optional list of instance ids to exclude
     *                         - if the model is already loaded in one of these
     * @param sync
     * @param returnStatus     whether an accurate loading status should be returned
     *                         (this parameter might be removed)
     */
    @Idempotent
//  @Override
    public StatusInfo ensureLoaded(String modelId, long lastUsed, List<String> excludeInstances, boolean sync,
            boolean returnStatus) throws ModelNotFoundException, InternalException {

        //TODO figure out how various failures (eg load failure) should be handled in sync/nonsync cases
        //  for async, decide where thread dispatch happens (ie should include all retries etc
        /// ... maybe should do it right after checking existence)

        try {
            return internalOperation(modelId, returnStatus, true, sync, lastUsed, excludeInstances);
        } catch (ModelLoadException mle) {
            return makeStatusInfo(Status.LOADING_FAILED, mle, null); // Should not reach here
        } catch (ModelNotHereException mnhe) {
            throw newInternalException("Unexpected ModelNotHereException", mnhe);
        }
    }

    /**
     * Returns the status of the specified model. See the {@link Status} enum.
     *
     * @param modelId
     */
    @Idempotent
//  @Override
    public StatusInfo getStatus(String modelId) throws InternalException {
        // secret commands to log or interrogate the state of this instance :-)
        if ("***LOGCACHE***".equals(modelId)) {
            for (String line : getCacheState()) logger.info(line);
        } else if ("***GETSTATE***".equals(modelId)) {
            return new StatusInfo(Status.NOT_CHECKED, getCacheState());
        }
        try {
            return internalOperation(modelId, true, false, false, -1L, null);
        } catch (ModelNotFoundException mnfe) {
            return SI_NOT_FOUND;
        } catch (ModelLoadException mle) {
            return makeStatusInfo(Status.LOADING_FAILED, mle, null); // Should not reach here
        } catch (ModelNotHereException mnhe) {
            throw newInternalException("Unexpected ModelNotHereException", mnhe);
        }
    }

    /* ----------------------------------------------------------------------------------------------------------- */

    protected String validateNewModelType(String modelType) throws InvalidInputException {
        if (Strings.isNullOrEmpty(modelType)) {
            throw new InvalidInputException("Must provide non-empty model type string");
        }
        if (acceptedModelTypes != null && !acceptedModelTypes.contains(modelType)) {
            throw new InvalidInputException("Unrecognized model type: " + modelType);
        }
        return modelType;
    }

    //TODO make use of InstanceRecord location in cache-miss placement

    abstract static class IdBasedLoadBalancer implements LoadBalancer {

        static class MapHolder {
            final Object[] arrRef;
            final Map<String, ServiceInstanceInfo> siMap;

            public MapHolder(Object[] arr) {
                this.arrRef = arr;
                this.siMap = Maps.newHashMapWithExpectedSize(arr.length);
                for (Object o : arr) {
                    ServiceInstanceInfo si = (ServiceInstanceInfo) o;
                    siMap.put(si.getInstanceId(), si);
                }
//              //TODO OR (maybe profile):
//              ImmutableMap.Builder<String, ServiceInstanceInfo> imb = ImmutableMap.builder();
//              for(ServiceInstanceInfo si : infos) imb.put(si.getInstanceId(), si);
//              siMap = imb.build();
            }
        }

        protected MapHolder mh = new MapHolder(new Object[0]);

        protected Map<String, ServiceInstanceInfo> getMap(Object[] arr) {
            MapHolder holder = mh;
            if (arr != holder.arrRef) {
                mh = holder = new MapHolder(arr);
            }
            return holder.siMap;
        }
    }

    // context parameters used on intra-cluster requests
    static final String TAS_INTERNAL_CXT_KEY = "tas.internal";
    static final String CACHE_HIT_EXCLUDES_KEY = "tas.ch_excludes";
    static final String CACHE_MISS_EXCLUDES_KEY = "tas.cm_excludes";
    static final String BATCH_COUNT_CXT_KEY = "tas.batch_mult"; // used only with hit_only
    static final String CHAINED_LOAD_COUNT_KEY = "tas.chain_load_count";
    static final String KNOWN_SIZE_CXT_KEY = "tas.known_size";
    static final String UNBALANCED_KEY = "mmesh.unbalanced";
    static final String DEST_INST_ID_KEY = "tas.dest_iid";
    static final String VMODELID = "vmodelid";

    // these are the possible values for the tas.internal context parameter
    // it won't be set on requests from outside of the cluster, and will
    // always be set to one of the below values for requests inside the cluster
    static final String HIT_ONLY = "hit_only", INTERNAL_REQ = "internal";
    static final String LOAD_LOCAL_ONLY = "load_local_only", FORCE_LOCAL_LOAD = "force_local_load";

    static final Splitter COMMA_SPLIT = Splitter.on(',');
    static final Joiner COMMA_JOIN = Joiner.on(',');

    static final Method internalOpRemoteMeth = getMethod(BaseModelMeshService.Iface.class, "internalOperation");

    /**
     * Used for intra-cluster ensureLoaded and getStatus invocations
     *
     * @param modelId
     * @param returnStatus     whether or not the model status is returned (might be cheaper not to obtain this if
     *                         not required). Currently this parameter has no effect - the status is <i>always</i> returned
     * @param load             whether or not loading of a new model should be triggered if no copies are currently loaded,
     *                         <i>or</i> the only accessible copies are in instances in the excludeInstances list
     * @param sync             only applies if load == true, dictates whether this method should block until the requested
     *                         model has completed loading (if excludeList is specified, this means wait until a copy in an instance
     *                         not on that list has completed loading)
     * @param excludeInstances specifies a list of instance ids to exclude when verifying that a copy of the
     *                         model is loaded and accessible, and will trigger loading of an <i>additional</i> copy if not (subject
     *                         to other constraints)
     */
    @Override
    public StatusInfo internalOperation(String modelId, boolean returnStatus, boolean load, boolean sync, long lastUsed,
            List<String> excludeInstances)
            throws ModelNotFoundException, ModelLoadException, ModelNotHereException, InternalException {
        try {
            return (StatusInfo) invokeModel(modelId, false, null,
                    internalOpRemoteMeth, returnStatus, load, sync, lastUsed, excludeInstances); // <-- "args"
        } catch (ModelNotFoundException | ModelLoadException | ModelNotHereException | InternalException e) {
            throw e;
        } catch (TException e) {
            throw newInternalException("Unexpected internalOperation exception for model " + modelId, e);
        }
    }

    /* -------------
     * DESIGN NOTES
     * -------------
     *
     * There are three disjoint "exclusion" lists of instances ids which may be attached to a
     * particular request as it traverses the model-mesh cluster:
     *
     * 1. The explicit excludeInstances list which is passed as a top-level service method
     *    parameter, and applies to ensureLoaded-type requests only. This list does not change
     *    between successive hops of a single top-level request. Instances in the list are
     *    effectively ignored for both the purposes of accessing existing loaded models and
     *    as targets for loading new copies of models.
     *
     * 2. A "cache hit exclude list" which travels as a comma-separated list in the
     *    CACHE_HIT_EXCLUDES_KEY litelinks threadcontext parameter, on intra-cluster requests only.
     *    The format is "id1,lt1,id2,lt2,..." where idN is an instance id, and ltN is the timestamp
     *    that the model was loaded in that corresponding instance.
     *
     *    Within processing of the invokeModel uber method, it's stored in a MapFilteringSet called
     *    "filtered", which is transferred to the ForwardingLB logic via the cacheHitExcludeTl
     *    ThreadLocal.
     *
     *    Instances in this list are ignored for the purposes of "cache hit" hops - i.e. they
     *    are instances that *do* have a copy of the model but couldn't be reached for some reason.
     *    Propagating this ensures problem instances won't be retried within the same request
     *    flow.
     *
     * 3. A "cache miss exclude list" which travels as a comma-separated list of instance ids
     *    in the CACHE_MISS_EXCLUDES_KEY litelinks threadcontext parameter, on intra-cluster
     *    "load_local_only" type requests only.
     *
     *    Within processing of the invokeModel uber method, it's stored in a CacheMissExcludeSet called
     *    "loadTargetFilter", which is shared with the CacheMissForwardingLB logic via the
     *    cacheMissExcludeTl ThreadLocal.
     *
     *    It keeps track of instances on which loading of the model has been unsuccessfully attempted
     *    in the same request flow (which will be instances that don't currently have a copy of the
     *    model loaded), so that they are excluded from any subsequent cache-miss requests.
     *
     */

    //TODO optimizations for ensureloaded and getstatus:
    //   infer loaded in case assoc instance exists + start-load time > MAX

    /**
     * Main uber method
     *
     * @param modelId
     * @param method     method to invoke on the runtime object (those returned by {@link ModelLoader#loadRuntime(String, ModelInfo)}),
     *                   or null in the ensureLoaded/getStatus case
     * @param remoteMeth thrift equivalent of method, declared on a subclass of {@link BaseModelMeshService.Iface},
     *                   or {@link #internalOpRemoteMeth} if method == null
     * @param args       either args corresponding to method, or if method == null then:
     *                   { boolean returnStatus, boolean load, boolean sync, long lastUsed, Collection&lt;String&gt; excludeInstances }
     *                   (see parameter descriptions for {@link #internalOperation(String, boolean, boolean, boolean, long, List)})
     * @return
     * @throws ModelNotFoundException
     * @throws TException
     */
    @SuppressWarnings("unchecked")
    protected Object invokeModel(final String modelId, Boolean isVModel, final Method method,
            final Method remoteMeth, final Object... args) throws ModelNotFoundException, ModelNotHereException, ModelLoadException, TException {

        //verify parameter values
        if (modelId == null || modelId.isEmpty()) {
            throw new ModelNotFoundException(modelId);
        }

        Map<String, String> contextMap = ThreadContext.getCurrentContext();
        if (contextMap == null) {
            contextMap = Collections.emptyMap();
        }

        final String tasInternal = contextMap.get(TAS_INTERNAL_CXT_KEY);
        String vModelId = "";
        if (isVModel) {
            vModelId = contextMap.get(VMODELID);
        }
        // Set the external request flag if it's not a tasInternal call or if
        // tasInternal == INTERNAL_REQ. The latter is a new ensureLoaded
        // invocation originating from within the cluster.
        final boolean externalReq;
        boolean local = false;
        final String hopType;
        if (tasInternal == null) {
            externalReq = true;
            hopType = "ex"; // "ex-ternal"
        } else if (INTERNAL_REQ.equals(tasInternal)) {
            externalReq = true;
            hopType = "in"; // "in-ternal"
        } else if (HIT_ONLY.equals(tasInternal)) {
            externalReq = false;
            local = true;
            hopType = "ho"; // "hit-only"
        } else {
            externalReq = false;
            hopType = "ll"; // "load-local"
        }

        // just for non-grpc api metric case
        long methodStartNanos = !isExternal && externalReq && method != null ? nanoTime() : 0L;
        Code metricStatusCode = Code.OK;

        final Thread curThread = Thread.currentThread();
        final String threadNameBefore = setThreadName(curThread, "invoke-" + hopType + '-' + modelId);
        ModelRecord mr = null;
        try {
            String destId = contextMap.get(DEST_INST_ID_KEY);
            if (destId != null) {
                // make sure destination id doesn't flow to any downstream reqs
                (contextMap = ensureContextMapIsMutable(contextMap)).remove(DEST_INST_ID_KEY);

                // If we aren't the intended destination, forward the request again. The assumption is that
                // this was from an instance *outside* of a Kubernetes cluster, and both us and the intended
                // destination are *inside* the Kubernetes cluster
                if (!instanceId.equals(destId)) {
                    if (directClient != null) {
                        logger.info("Forwarding mis-directed request for model " + modelId + " to inst " + destId);
                        return forwardInvokeModel(destId, modelId, remoteMeth, args);
                    }
                    logger.warn("Received invokeModel request with incorrect dest inst id: " + destId + " (we are "
                                + instanceId + ")");
                }
            }

            // this is -1 in getStatus case (get quietly), 0 means "now"
            long lastUsedTime = method != null ? 0L : (Long) args[3];

            if (lastUsedTime < 0L && (Boolean) args[1]) {
                logger.warn("model load requested with lastUsedTime < 0 (" + lastUsedTime + "), treating as 0 (now)");
                lastUsedTime = 0L;
            }

            if (logger.isDebugEnabled()) {
                String knownSize = contextMap.get(KNOWN_SIZE_CXT_KEY);
                logger.debug("invokeModel for model " + modelId + " meth="
                             + (method != null ? method.getName() : "null") + " tasInternal=" + tasInternal
                             + (method != null ? "" : " lastUsed=" + lastUsedTime + " excl=" + args[4])
                             + (knownSize != null ? " knownSize=" + knownSize : "") + " instance=" + instanceId);
            }

            // check if local flag has been set
            // when the local flag is set cache entry is fetched from local instance's cache only
            // if not found then an exception is thrown
            if (local) {
                CacheEntry<?> ce = getFromCache(modelId, lastUsedTime);
                if (ce == null) {
                    throw new ModelNotHereException(instanceId, modelId);
                }
                try {
                    return invokeLocalModel(ce, method, args, modelId, isVModel);
                } catch (ModelLoadException mle) {
                    mr = registry.get(modelId);
                    if (mr == null || !mr.loadFailedInInstance(instanceId)) {
                        if (handleUnexpectedFailedCacheEntry(ce, mr) != mr) {
                            // entry was removed, treat like it wasn't here
                            throw new ModelNotHereException(instanceId, modelId);
                        }
                    }
                    throw mle;
                }
            }

            // Determine if this request was "balanced" at the source
            // (default assumption unless mmesh.unbalanced context parameter is set)
            // If so, we always choose ourselves for cache hits and new model placements,
            // as long as we're a suitable "candidate". This reduces the total number of
            // network hops.
            final boolean sourceIsBalanced = !"true".equals(contextMap.get(UNBALANCED_KEY));
            final boolean favourSelfForHits = (!limitModelConcurrency && sourceIsBalanced) || method == null;
            final boolean preferSelfForHits = !favourSelfForHits && sourceIsBalanced;

            if (!sourceIsBalanced) {
                // don't pass unbalanced flag downstream
                contextMap = ensureContextMapIsMutable(contextMap);
                contextMap.remove(UNBALANCED_KEY);
            }

            // get the model record using the model id
            mr = getModelRecord(modelId);

            // get the cache hit exclude and cache miss exclude parameters
            String cacheHitExcludeString = null, cacheMissExcludeString = null;
            if (contextMap.containsKey(CACHE_HIT_EXCLUDES_KEY) || contextMap.containsKey(CACHE_MISS_EXCLUDES_KEY)) {
                contextMap = ensureContextMapIsMutable(contextMap);
                cacheHitExcludeString = contextMap.remove(CACHE_HIT_EXCLUDES_KEY);
                cacheMissExcludeString = contextMap.remove(CACHE_MISS_EXCLUDES_KEY);
            }

            // Get filtered map of instances
            // The excludeSelf flag is set to true if we're favouring ourselves (i.e. in source balanced
            // case) since local invocations will then be determined outside of the load balancer
            final MapFilteringSet<String, Long> filtered = new MapFilteringSet<>(
                    favourSelfForHits, preferSelfForHits, mr.getType());
            Collection<String> explicitExcludes = null;
            if (method == null) {
                explicitExcludes = (Collection<String>) args[4];
                if (explicitExcludes != null && !explicitExcludes.isEmpty()) {
                    filtered.setKeyExcludes(explicitExcludes.size() <= 4 || (explicitExcludes instanceof Set) ?
                            explicitExcludes : (explicitExcludes = new HashSet<>(explicitExcludes)));
                }
            }

            // add instances from cache hit exclude to the map of filtered instances
            if (cacheHitExcludeString != null) {
                for (Iterator<String> it = COMMA_SPLIT.split(cacheHitExcludeString).iterator(); it.hasNext(); ) {
                    try {
                        filtered.add(it.next(), Long.valueOf(it.next()));
                    } catch (NumberFormatException | NoSuchElementException e) {
                        throw newInternalException("Invalid cache hit exclude string: " + cacheHitExcludeString, e);
                    }
                }
            }

            // cache hit filtering is set union view of:
            // 1- explicit excludes (if any)
            // 2- passed in cache-hit exclude list (if any)
            // 3- "accumulated" excludes from attempts in current call (if any)
            //
            // downstream cache-miss calls will have cache-hitstring passed as conversion of #3 to string list

            // cache miss filtering is set union view of:
            // 1,2- model rec instances, model rec failures (apart from maybe "expired" failures, TBD)  (if any)
            // 3- "accumulated" excludes from attempts in current call (if any)

            // remove instances to filter from the instance ids of the model
            Map<String, Long> filteredInstances = filtered.filteredMap(filterIfReadOnly(mr.getInstanceIds()));
            // System.out.println("DEBUG model="+modelId+" filtered="+filteredInstances);

            CacheMissExcludeSet loadTargetFilter = null;
            boolean cacheHitTlSet = false, cacheMissTlSet = false;
            ModelLoadException loadFailureSeen = null;
            TException internalFailureSeen = null; // ONLY InternalException or ApplierException
            CacheEntry<?> cacheEntry = null;
            int resExaustedCount = 0; // counts RESOURCE_EXCEEDED errors and fails if there are 3
            try {
                // ---------- main / cache-hit loop ------------
                for (int n = 0; n < MAX_ITERATIONS; n++) { // the max is just to protect against bugs

                    // check for global cache "hit" - where registry says there is at least one
                    // instance id associated with the model as per our current view of the system
                    int filteredCount = filteredInstances.size();
                    if (filteredCount > 0) {
                        // already loaded/loading "somewhere"

                        Long localLoaded = filteredInstances.get(instanceId);

                        boolean goLocal = false;
                        if (localLoaded != null) {
                            goLocal = filteredCount == 1;
                            if (!goLocal && favourSelfForHits) {
                                if (cacheEntry == null) {
                                    cacheEntry = getFromCache(modelId, lastUsedTime); // get local instance
                                }

                                // if loaded here OR [loading here and unlikely to be loaded elsewhere], serve.
                                if (cacheEntry != null) {
                                    if (cacheEntry.isDone()) {
                                        goLocal = true;
                                    } else {
                                        long oldest = oldest(filteredInstances);
                                        // if we are oldest OR oldest is < 1.5sec old
                                        if (oldest == localLoaded || age(oldest) < 1500L) {
                                            goLocal = true;
                                        }
                                    }
                                }
                            }
                        }

                        if (!goLocal) {
                            // here we might be sending elsewhere in the cluster
                            // (or definitely so in balanced case, where we would have
                            // already chosen goLocal if a local copy was available)

                            if (!cacheHitTlSet) {
                                cacheHitExcludeTl.set(filtered); // pass instances to LB
                                cacheHitTlSet = true;
                            }
                            try {
                                // call "local only" version of prediction
                                contextMap = ensureContextMapIsMutable(contextMap);
                                contextMap.put(TAS_INTERNAL_CXT_KEY, HIT_ONLY);
                                if (logger.isDebugEnabled()) {
                                    logger.debug("model=" + modelId + " about to invoke other instances. tofilter="
                                                 + filtered);
                                }
                                Object result = invokeRemote(runtimeClient, method, remoteMeth, modelId, args);
                                return method == null && externalReq ? updateWithModelCopyInfo(result, mr) : result;
                            } catch (Exception e) {
                                final Throwable t = e instanceof InvocationTargetException ? e.getCause() : e;
                                final boolean callFailed = processRemoteInvocationException(t, modelId); // this may throw
                                if (callFailed) {
                                    if (t instanceof ModelLoadException) {
                                        loadFailureSeen = (ModelLoadException) t;
                                        updateLocalModelRecordAfterRemoteLoadFailure(mr, loadFailureSeen);
                                    } else if (t instanceof InternalException) {
                                        internalFailureSeen = (InternalException) t;
                                    } else if (isExhausted(t) && ++resExaustedCount >= MAX_RES_EXHAUSTED) {
                                        Throwables.throwIfInstanceOf(t, Error.class);
                                        Throwables.throwIfInstanceOf(t, Exception.class);
                                        throw new IllegalStateException(t); // should not happen
                                    }
                                    continue;
                                }
                                // else load balancer returned null which means either:
                                //  - we haven't tried local yet and should try that next
                                //  - or, there are no more instances to try (will fall through to cache miss section)
                                goLocal = localLoaded != null && filteredInstances.containsKey(instanceId);
                            } finally {
                                if (sendDestinationId) {
                                    // thread context *may* have changed inside the LB logic, must ensure synced
                                    contextMap = ThreadContext.getCurrentContext();
                                    if (contextMap.containsKey(DEST_INST_ID_KEY)) {
                                        (contextMap = ensureContextMapIsMutable(contextMap)).remove(DEST_INST_ID_KEY);
                                    }
                                }
                            }
                        }

                        if (goLocal) {
                            try {
                                if (cacheEntry == null) {
                                    cacheEntry = getFromCache(modelId, lastUsedTime); // get local instance
                                    if (cacheEntry == null) {
                                        // if not found, remove from KV record (shouldn't happen) + continue
                                        mr.getInstanceIds().remove(instanceId);
                                        mr.updateLastUsed(lastUsedTime);
                                        try {
                                            ModelRecord existMr = registry.conditionalSetAndGet(modelId, mr);
                                            if (existMr != mr) {
                                                filteredInstances = refreshAfterUpdatedModelRecord(modelId, mr = existMr,
                                                        filtered, loadTargetFilter, externalReq, contextMap);
                                            }
                                            continue;
                                        } catch (Exception e) {
                                            // KV store error - we can't fix the record so treat as a loading error
                                            filtered.add(instanceId, localLoaded);
                                            ModelLoadException mle = newModelLoadException(
                                                    "KV store error attempting to prune model record: " + e,
                                                    KVSTORE_LOAD_FAILURE, e);
                                            if (io.grpc.Status.fromThrowable(e).getCode() != Code.CANCELLED) {
                                                CacheEntry<?> failedEntry = new CacheEntry<>(modelId, mr, mle);
                                                cacheEntry = unloadManager != null
                                                        ? unloadManager.insertFailedPlaceholderEntry(modelId, failedEntry, mr.getLastUsed())
                                                        : runtimeCache.putIfAbsent(modelId, failedEntry, mr.getLastUsed());
                                            }
                                            if (cacheEntry == null) {
                                                throw mle;
                                            }
                                            // else fall-through
                                        }
                                    }
                                }
                                filtered.add(instanceId, localLoaded);
                                if (!favourSelfForHits) {
                                    // unbalanced source => additional tracking of local
                                    // invocations for use in ForwardingLB
                                    if (!preferSelfForHits) {
                                        lastInvokeTime = currentTimeMillis();
                                    }
                                    localInvokesInFlight.incrementAndGet();
                                }
                                try {
                                    Object result = invokeLocalModel(cacheEntry, method, args, modelId, isVModel);
                                    return method == null && externalReq ? updateWithModelCopyInfo(result, mr) : result;
                                } finally {
                                    if (!favourSelfForHits) {
                                        localInvokesInFlight.decrementAndGet();
                                    }
                                }
                            } catch (ModelNotHereException mnhe) {
                                if (cacheEntry != null && cacheEntry.isFinished()) {
                                    mr.getInstanceIds().remove(instanceId, cacheEntry.loadTimestamp);
                                }
                                continue;
                            } catch (ModelLoadException mle) {
                                ModelRecord newMr = handleUnexpectedFailedCacheEntry(cacheEntry, mr);
                                if (newMr != mr) {
                                    filtered.remove(instanceId, localLoaded);
                                    filteredInstances = refreshAfterUpdatedModelRecord(modelId, mr,
                                            filtered, loadTargetFilter, externalReq, contextMap);
                                } else {
                                    loadFailureSeen = mle;
                                }
                                continue;
                            } catch (InternalException ie) {
                                if (isInterruption(ie)) {
                                    throw ie;
                                }
                                internalFailureSeen = ie;
                                continue;
                            } catch (ApplierException ae) {
                                if (!isExhausted(ae) || ++resExaustedCount >= MAX_RES_EXHAUSTED) {
                                    throw ae;
                                }
                                internalFailureSeen = ae;
                                continue;
                            }
                        }
                    }

                    // Global cache miss! (or possibly failed to invoke at all "registered" locations)

                    if (method == null && !(Boolean) args[1] /* this is the "load" arg */) {
                        // getStatus case
                        if (loadFailureSeen != null || mr.hasLoadFailure()) {
                            return makeStatusInfo(Status.LOADING_FAILED, loadFailureSeen, mr);
                        }
                        return SI_NOT_LOADED;
                    }

                    // don't attempt to load if there already are >= MAX_LOAD_FAILURES recent load failures (these will expire)
                    checkLoadFailureCount(mr, loadFailureSeen);

                    // don't attempt load if failed to invoke in > N already loaded locations
                    checkLoadLocationCount(mr, explicitExcludes, internalFailureSeen);

                    if (externalReq) { // tas.internal != LOAD_LOCAL_ONLY
                        // CacheMissExcludeSet loadTargetFilter created and updated with
                        //   cacheMissExclude, explicit, loaded & failed instances
                        if (loadTargetFilter == null) {
                            // this might be moved out of the if(externalReq) to support the
                            // not-yet-added "internal" suitable candidate check below
                            loadTargetFilter = new CacheMissExcludeSet(sourceIsBalanced, mr.getType());
                            cacheMissExcludeTl.set(loadTargetFilter);
                            cacheMissTlSet = true;
                            updateCacheMissExcludeSet(loadTargetFilter, cacheMissExcludeString,
                                    explicitExcludes, mr, lastUsedTime);
                        }

                        //add CASH_HIT_EXCLUDES_KEY-->CsvString(filtered) to the contextMap
                        contextMap = addFilterMapToContext(contextMap, filtered);

                        contextMap = ensureContextMapIsMutable(contextMap);
                        contextMap.put(TAS_INTERNAL_CXT_KEY, LOAD_LOCAL_ONLY);
                    }
                    // These are LOAD_LOCAL_ONLY cases (we are not the req ingress), where the model is actually
                    // found to be loaded. It must either be a failed load or our invocation of it failed.
                    else if (mr.getInstanceIds().containsKey(instanceId)) {
                        if (internalFailureSeen != null) throw internalFailureSeen;
                        if (loadFailureSeen != null) throw loadFailureSeen;
                        throw newInternalException("Cluster routing state error", null); // shouldn't happen
                    } else if (mr.loadFailedInInstance(instanceId)) {
                        if (loadFailureSeen != null) throw loadFailureSeen;
                        throw new ModelLoadException(mr.getLoadFailureMessage(instanceId), instanceId, 0L, null);
                    }

                    // -------- cache miss loop ----------
                    while (true) {

                        // Pre-check type constraints (if applicable)
                        verifyLocalTypeConstraintsBeforeLoad(mr, externalReq);

                        if (externalReq) { // tas.internal != LOAD_LOCAL_ONLY
                            try {
                                // this will throw ServiceUnavailableException (rather than actually making
                                // any remote invocation) if the LB logic decides it should be loaded locally
                                Object result = invokeRemote(cacheMissClient, method, remoteMeth, modelId, args);
                                return method == null && externalReq ? updateWithModelCopyInfo(result, mr) : result;
                            } catch (Exception e) {
                                final Throwable t = e instanceof InvocationTargetException ? e.getCause() : e;
                                final boolean callFailed = processRemoteInvocationException(t, modelId); // this may throw
                                //TODO handle "stale" case here
                                if (callFailed) {
                                    if (t instanceof ModelLoadException) {
                                        loadFailureSeen = (ModelLoadException) t;
                                        updateLocalModelRecordAfterRemoteLoadFailure(mr, loadFailureSeen);
                                    } else if (t instanceof InternalException) {
                                        internalFailureSeen = (InternalException) t;
                                    } else if (isExhausted(t) && ++resExaustedCount >= MAX_RES_EXHAUSTED) {
                                        Throwables.throwIfInstanceOf(t, Error.class);
                                        Throwables.throwIfInstanceOf(t, Exception.class);
                                        throw new IllegalStateException(t); // should not happen
                                    }
                                    // continue inner loop
                                    if (++n >= MAX_ITERATIONS) {
                                        ModelNotHereException cacheOverflow = e instanceof ModelNotHereException
                                                ? (ModelNotHereException) e : null;
                                        throw iterationsExhausted(internalFailureSeen, loadFailureSeen,
                                                cacheOverflow, modelId);
                                    }
                                    continue;
                                }
                                // else load balancer returned null - this means either the local instance was
                                // chosen as the target, no other instances were found/worked -- so carry on
                            } finally {
                                // thread context *may* have changed inside the LB logic, must ensure synced
                                contextMap = ThreadContext.getCurrentContext();
                                if (sendDestinationId && contextMap.containsKey(DEST_INST_ID_KEY)) {
                                    (contextMap = ensureContextMapIsMutable(contextMap)).remove(DEST_INST_ID_KEY);
                                }
                            }
                        } else {
                            // this is an internal "load" request (forwarded from another cluster instance)
                            if (!FORCE_LOCAL_LOAD.equals(tasInternal)) {
                                //  here test if this instance is considered a suitable candidate,
                                //  follow same logic as in CacheMissForwardingLB
                                if (false) { //TODO !!!
                                    // not suitable candidate
                                    publishInstanceRecord(true, false); // not async, so that it will be seen by caller
                                    throw new ModelLoadException(); //TODO .. should specifically reflect "stale info"
                                }
                            }
                        }

                        // here either the local instance was chosen as the target,
                        // no other instances were found/worked,
                        // or it's an internal req meaning the model can only be loaded locally
                        throwIfLocalLoadNotAllowed(modelId, externalReq, mr, loadTargetFilter,
                                loadFailureSeen, internalFailureSeen);

                        // limit the rate of cache churn - if we are full and our LRU entry is recent,
                        // reject the load rather than thrashing
                        if (minChurnAgeMs > 0) {
                            long remaining = runtimeCache.capacity() - runtimeCache.weightedSize();
                            if (isFull(remaining)) {
                                // this instance is "full"
                                long lru = runtimeCache.oldestTime();
                                if (lru >= 0 && lru != Long.MAX_VALUE && age(lru) < minChurnAgeMs) {
                                    logger.warn("Rejecting attempt to load new model " + modelId + " in instance "
                                                + instanceId + " because oldest loaded is too recent: "
                                                + getFreshInstanceRecord());
                                    throw newModelLoadException("Cache churn threshold exceeded", 0L, null);
                                }
                            }
                        }

                        //TODO also consider what to do if loadingPool is full (maybe "soft reject" like stale info case)

                        // Attempt to load model locally and then invoke it

                        // create model record holder
                        ModelRecord[] mrHolder = { mr }; // "pass by reference"

                        // prioritize loading if this is a runtime req (rather than preemptive load)
                        boolean prioritize = method != null;

                        // call loadLocal - only throws KV-store Exceptions, InterruptedException
                        cacheEntry = loadLocal(modelId, mrHolder, lastUsedTime, contextMap, prioritize);

                        // MR may have changed inside the method
                        mr = mrHolder[0];

                        // verify cache entry
                        if (cacheEntry == INSTANCES_CHANGED) {
                            // this means load was aborted because the model registrations changed
                            cacheEntry = null;
                            filteredInstances = refreshAfterUpdatedModelRecord(modelId, mr,
                                    filtered, loadTargetFilter, externalReq, contextMap);
                            break; // this will continue outer loop (go back to cache hit section)
                        }

                        if (loadTargetFilter != null) {
                            loadTargetFilter.add(instanceId);
                        }

                        if (cacheEntry == null) {
                            // This means the load failed because the cache entry was immediately evicted
                            // (older than all the existing entries in the cache)
                            if (method == null) {
                                ModelNotHereException mnhe = new ModelNotHereException(instanceId, modelId);
                                if (!externalReq) {
                                    throw mnhe;
                                }
                                if (++n >= MAX_ITERATIONS) {
                                    throw iterationsExhausted(internalFailureSeen, loadFailureSeen, mnhe, modelId);
                                }
                                continue; // continue to try, may still fit on others
                            }
                            // Not expected to reach here, should only happen in ensureLoaded case
                            throw newModelLoadException("Entry too old for cache for model " + modelId, 0L, null);
                        }

                        // local loading was triggered successfully - if this is a runtime request, trigger
                        // load of a second copy if the cluster is in an appropriate state
                        // -- not currently applicable since we force a 2 second delay for loading a second copy
//                        if(lastUsedTime >= 0) {
//                            addSecondCopyIfNecessary(modelId, cacheEntry.getWeight(), mr.getInstanceIds(),
//                                    lastUsedTime, currentTimeMillis(), false);
//                        }

                        // invoke model
                        try {
                            Object result = invokeLocalModel(cacheEntry, method, args, modelId, isVModel);
                            return method == null && externalReq ? updateWithModelCopyInfo(result, mr) : result;
                        } catch (ModelNotHereException e) {
                            if (loadTargetFilter != null) loadTargetFilter.remove(instanceId);
                            if (cacheEntry.isFinished()) {
                                mr.getInstanceIds().remove(instanceId, cacheEntry.loadTimestamp);
                            }
                            logger.warn("Model " + modelId + " removed while waiting for locally-triggered load");
                        } catch (ModelLoadException mle) {
                            if (!externalReq) throw mle;
                            loadFailureSeen = mle;
                            logger.error("Local invocation failed for model " + modelId, mle);
                        } catch (InternalException ie) {
                            if (!externalReq || isInterruption(ie)) throw ie;
                            internalFailureSeen = ie;
                            logger.error("Local invocation failed for model " + modelId, ie);
                        } catch (ApplierException ae) {
                            if (!externalReq || !isExhausted(ae) || ++resExaustedCount >= MAX_RES_EXHAUSTED) throw ae;
                            internalFailureSeen = ae;
                        }

                        // continue inner loop
                        if (++n >= MAX_ITERATIONS) {
                            throw iterationsExhausted(internalFailureSeen, loadFailureSeen, null, modelId);
                        }
                    }
                }
                throw iterationsExhausted(internalFailureSeen, loadFailureSeen, null, modelId);
            } finally {
                if (cacheHitTlSet) cacheHitExcludeTl.set(null);
                if (cacheMissTlSet) cacheMissExcludeTl.set(null);
            }

        } catch (InterruptedException ie) {
            metricStatusCode = Code.CANCELLED;
            throw newInternalInterruptedException(ie, "load of model " + modelId);
        } catch (Exception e) {
            metricStatusCode = methodStartNanos > 0L && isInterruption(e) ? Code.CANCELLED : Code.UNKNOWN;
            if (method == null && externalReq && e instanceof ModelLoadException) {
                return makeStatusInfo(Status.LOADING_FAILED, (ModelLoadException) e, mr);
            }
            if (e instanceof TException) {
                throw (TException) e;
            }

            // etcd/Zookeeper exceptions (& RuntimeExceptions maybe)
            //TODO consider in what cases we can tolerate etcd/zookeeper absence and still respond
            throw newInternalException("Unexpected exception in invokeModel method for model " + modelId, e);
        } catch (Throwable t) {
            metricStatusCode = Code.INTERNAL;
            throw t;
        } finally {
            if (methodStartNanos > 0L && metrics.isEnabled()) {
                // only logged here in non-grpc (legacy) mode
                metrics.logRequestMetrics(true, getRequestMethodName(method, args),
                    nanoTime() - methodStartNanos, metricStatusCode, -1, -1, modelId, vModelId);
            }
            curThread.setName(threadNameBefore);
        }
    }

    private void throwIfLocalLoadNotAllowed(String modelId, boolean externalReq, ModelRecord mr,
                                            CacheMissExcludeSet loadTargetFilter,
                                            ModelLoadException loadFailureSeen, TException internalFailureSeen) throws TException {
        // Called after the decision has been made to attempt to load the model on the local instance

        // We need to check that
        // - The load target filter doesn't exclude us
        // - If there are type constraints, that they don't exclude us
        boolean localFiltered = loadTargetFilter != null && loadTargetFilter.isExcluded(instanceId);
        Set<String> constrainTo;
        boolean blockedByTypeConstraint = typeConstraints != null
                && (constrainTo = typeConstraints.getCandidateInstances(mr.getType())) != null
                && !constrainTo.contains(instanceId);
        if (localFiltered || blockedByTypeConstraint) {
            // We're not eligible to load the model, figure out why and throw an exception
            if (!externalReq) {
                // should not be LOAD_LOCAL_ONLY && local filtered
                throw newInternalException("Cluster routing error", null);
            }
            // here loading failed everywhere
            if (loadFailureSeen != null) {
                throw loadFailureSeen;
            }
            if (mr.hasLoadFailure()) {
                String failInstance = getMostRecent(mr.getLoadFailedInstanceIds());
                throw new ModelLoadException(mr.getLoadFailureMessage(failInstance),
                        failInstance, 0L, null);
            }
            if (internalFailureSeen != null) {
                throw internalFailureSeen;
            }
            // Reaching here is unexpected and implies some kind of state inconsistency.
            // Log with some additional diagnostic info.
            logger.warn("Nowhere available to load for model " + modelId + ": type=" + mr.getType()
                + ", constraintBlocked=" + blockedByTypeConstraint + ", loadTargetFilter=" + loadTargetFilter
                    + ", instanceTable=" + Iterables.toString(instanceInfo.keyIterable()));
            //maybe define specific exception
            throw new ModelLoadException("Nowhere available to load", null, 0L, null);
        }
    }

    private static void updateLocalModelRecordAfterRemoteLoadFailure(ModelRecord mr, ModelLoadException loadFailure) {
        // The model copy we attempted to invoke must have failed to load in the time
        // since we retrieved its record. Update our local copy of the record for now
        // to reflect this.
        String failInstance = loadFailure.getInstanceId();
        if (Strings.isNullOrEmpty(failInstance) && !mr.loadFailedInInstance(failInstance)) {
            mr.addLoadFailure(failInstance, currentTimeMillis(), loadFailure.getMessage());
        }
    }

    private static StatusInfo updateWithModelCopyInfo(Object statusInfo, ModelRecord mr) {
        StatusInfo si = (StatusInfo) statusInfo;
        Status status = si.getStatus();
        // Fill-in additional model copy info
        return status == Status.LOADED || status == Status.LOADING ? makeStatusInfo(status, null, mr) : si;
    }

    private void verifyLocalTypeConstraintsBeforeLoad(ModelRecord mr, boolean externalReq) throws ModelLoadException {
        if (typeConstraints != null) {
            String type = mr.getType();
            Set<String> constrainTo = typeConstraints.getCandidateInstances(type);
            if (constrainTo != null) {
                if (externalReq) {
                    if (constrainTo.isEmpty()) {
                        throw new ModelLoadException("There are no running instances that"
                                                     + " meet the label requirements of type " + type + ": "
                                                     + Arrays.toString(typeConstraints.getRequiredLabels(type)),
                                null, 0L, null);
                    }
                } else if (!constrainTo.contains(instanceId)) {
                    // should not reach here as long as all instances see same type-label
                    // constraint mappings (since it's an internal request)
                    throw new ModelLoadException("Instance does not have all required labels"
                                                 + " for type " + type + ": "
                                                 + Arrays.toString(typeConstraints.getRequiredLabels(type)),
                            instanceId, 0L, null);
                }
            }
        }
    }

    // Called from the main invokeModel method to update request-scoped derived state in local
    // vars and the thread context, after a new ModelRecord has been retrieved from the registry.
    // returns filteredInstances
    private Map<String, Long> refreshAfterUpdatedModelRecord(String modelId, ModelRecord mr,
            MapFilteringSet<String, Long> filtered, CacheMissExcludeSet loadTargetFilter,
            boolean externalReq, Map<String, String> contextMap) throws ModelNotFoundException {

        if (mr == null) {
            throw new ModelNotFoundException(modelId); // not found
        }
        if (loadTargetFilter != null) {
            loadTargetFilter.updateLoadedAndFailedInstances(mr);
        }
        if (externalReq) {
            // contains checks as precaution in case immutable
            if (contextMap.containsKey(CACHE_HIT_EXCLUDES_KEY)) contextMap.remove(CACHE_HIT_EXCLUDES_KEY);
            if (contextMap.containsKey(CACHE_MISS_EXCLUDES_KEY)) contextMap.remove(CACHE_MISS_EXCLUDES_KEY);
        }
        return filtered.filteredMap(filterIfReadOnly(mr.getInstanceIds()));
    }

    private static TException iterationsExhausted(TException internalFailureSeen, ModelLoadException loadFailureSeen,
            ModelNotHereException cacheOverflow, String modelId) {
        logger.warn(MAX_ITERATIONS + " retry iterations exhausted for model " + modelId);
        if (loadFailureSeen != null) return loadFailureSeen;
        if (internalFailureSeen != null) return internalFailureSeen;
        if (cacheOverflow != null) return cacheOverflow;
        return newInternalException(MAX_ITERATIONS + " retry iterations exhausted", null);
    }

    private Map<String, Long> filterIfReadOnly(Map<String, Long> instId) {
        return !readOnlyMode ? instId : Maps.filterKeys(instId, instanceInfo::contains);
    }

    protected static final ThreadLocal<String> destinationInstance = new ThreadLocal<>();

    /**
     * Forward an invokeModel request to a *specific* instance based on id. Currently
     * only used when the model-mesh cluster is spanning a kubernetes cluster - i.e. some
     * instances inside and some out, and a request has been sent from outside the
     * cluster to an instance inside (since it may land on an unintended instance in
     * that case).
     * @param isVModel TODO
     * @throws TException TODO
     * @throws ModelNotHereException if the specified destination instance isn't found
     */
    protected Object forwardInvokeModel(String destId, String modelId, Method remoteMeth, Object... args)
            throws TException {
        destinationInstance.set(destId);
        try {
            //TODO: not sure what is happening here.. do I need to pass vmodelid to the remoteMeth.invoke?
            return remoteMeth.invoke(directClient, ObjectArrays.concat(modelId, args));
        } catch (Exception e) {
            if (e instanceof InvocationTargetException) {
                Throwable t = e.getCause();
                if (t.getCause() instanceof ServiceUnavailableException) {
                    throw new ModelNotHereException(destId, modelId);
                } else if (t instanceof Error) {
                    throw (Error) t;
                } else {
                    throw (TException) t;
                }
            }
            throw newInternalException("Unexpected exception forwarding req for model " + modelId + " to " + destId, e);
        } finally {
            destinationInstance.remove();
        }
    }

    /**
     * @param modelId
     * @param lastUsed if -1, don't "touch" the cache entry
     * @return
     */
    protected CacheEntry<?> getFromCache(String modelId, long lastUsed) {
        return lastUsed >= 0L ? runtimeCache.get(modelId, lastUsed) : runtimeCache.getQuietly(modelId);
    }

    // 0 means "now"
    protected static long age(long timeMillis) {
        return timeMillis == 0 ? 0L : currentTimeMillis() - timeMillis;
    }

    protected static long oldest(Map<String, Long> instances) {
        return Collections.min(instances.values());
    }

    private ModelRecord getModelRecord(String modelId) throws Exception {
        ModelRecord mr = registry.getOrStrongIfAbsent(modelId);
        if (mr == null) {
            throw new ModelNotFoundException(modelId); // not found
        }
        return mr;
    }

    // For a given contextMap and a specific key
    // add instances in comma separated form as value
    private static Map<String, String> addFilterMapToContext(Map<String, String> contextMap,
            MapFilteringSet<String, Long> filteredInstances) {

        if (!filteredInstances.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (Entry<String, Long> ent : filteredInstances.keySet()) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append(ent.getKey()).append(',').append(ent.getValue());
            }
            contextMap = ensureContextMapIsMutable(contextMap);
            contextMap.put(CACHE_HIT_EXCLUDES_KEY, sb.toString());
        }

        return contextMap;
    }

    /*
     * The incoming litelinks ThreadContext map will either
     * be immutable or null, so we must replace with a new map in order to
     * to mutate it.
     *
     * However that may have already been done (i.e. by this method).
     *
     * In this case, it will be a HashMap so we don't need to create a new one.
     */
    // package-private to avoid synthetic
    static Map<String, String> ensureContextMapIsMutable(Map<String, String> contextMap) {
        if (contextMap == null) {
            ThreadContext.setCurrentContext(contextMap = new HashMap<>(8));
        } else if (!(contextMap instanceof HashMap)) {
            ThreadContext.setCurrentContext(contextMap = new HashMap<>(contextMap));
        }
        return contextMap;
    }

    /**
     * @param t
     * @return true if remote call failed, false if call wasn't made (due to unavailability or
     * indication that local attempt should be made)
     * @throws TException
     */
    protected boolean processRemoteInvocationException(Throwable t, String modelId) throws TException {
        if (t instanceof IllegalAccessException || t instanceof RuntimeException) {
            throw newInternalException(
                    "Unexpected exception while attempting remote invocation for model " + modelId, t);
        } else {
            if (t.getCause() instanceof ServiceUnavailableException) {
                return false;
            } else if (t instanceof ModelNotHereException) {
                ModelNotHereException mnhe = (ModelNotHereException) t;
                logger.info("Received ModelNotHereException from remote invocation" + " for model " + mnhe.getModelId()
                            + " in instance " + mnhe.getInstanceId());
                return true;
            } else if (t instanceof ModelLoadException) {
                logger.error("Remote invocation failed for model " + modelId, t);
                return true;
            } else if (t instanceof InternalException
                       || t instanceof TTransportException
                       || t instanceof TProtocolException
                       || t instanceof TApplicationException
                       || t instanceof RuntimeException) {
                Exception interrupted = getInterruptionCause(t);
                if (interrupted != null) {
                    throw newInternalInterruptedException(interrupted, "remote invocation for model " + modelId);
                }
                logger.error("Remote invocation failed for model " + modelId, t);
                return true;
            } else if (t instanceof ApplierException) {
                ApplierException ae = (ApplierException) t;
                if (RESOURCE_EXHAUSTED.equals(ae.getGrpcStatusCode())) {
                    return true;
                } else {
                    throw ae;
                }
            }
            Throwables.throwIfInstanceOf(t, Error.class);
            Throwables.throwIfInstanceOf(t, TException.class); // other app-defined exceptions or ModelNotFoundException
            throw new IllegalStateException(t); // should not happen
        }
    }

    /* ----------------------------------- global "cache hit" related -------------------------------------------- */

    static class MapFilteringSet<K, V> extends HashMap/*TreeMap*/<Entry<K, V>, Boolean>
            implements Predicate<Entry<K, V>> {
        final boolean excludeSelf, preferSelf;
        final String modelType;
        private Map<K, V> map;
        private Collection<K> keyExcludes;

        public MapFilteringSet(boolean excludeSelf, boolean preferSelf, String modelType) {
            super(4);
            // assert !(excludeSelf && preferSelf)
            this.excludeSelf = excludeSelf;
            this.preferSelf = preferSelf;
            this.modelType = modelType;
        }

        @Override
        public boolean apply(Entry<K, V> input) {
            return !containsKey(input) && (keyExcludes == null || !keyExcludes.contains(input.getKey()));
        }

        public boolean add(K key, V value) {
            return put(Maps.immutableEntry(key, value), Boolean.TRUE) == null;
        }

        public Map<K, V> map() {
            return map;
        }

        public Map<K, V> filteredMap(Map<K, V> source) {
            return map = Maps.filterEntries(source, this); //Maps.filterKeys(source, this);
        }

        public void setKeyExcludes(Collection<K> excludes) {
            keyExcludes = excludes;
        }
    }

    // these are only used in the context of *unbalanced* cache hits
    private final AtomicInteger localInvokesInFlight = new AtomicInteger();
    private long lastInvokeTime;

    // load balancer used to forward to instances which (should) already have the model loaded
    protected static final ThreadLocal<MapFilteringSet<String, Long>> cacheHitExcludeTl = new ThreadLocal<>();

    class ForwardingLB extends IdBasedLoadBalancer {

        // maybe make this static, with instanceId field

        @SuppressWarnings("unchecked")
        @Override
        public <T> T getNext(Object[] sis, String method, Object[] args) {
            final MapFilteringSet<String, Long> filtered = cacheHitExcludeTl.get();
            final Map<String, Long> filteredInstances = filtered.map();
            if (filteredInstances == null || filteredInstances.isEmpty()) {
                logger.warn("ForwardingLB received no instances");
                return null;
            }
            final boolean excludeSelf = filtered.excludeSelf, preferSelf = filtered.preferSelf;
            boolean seenSelf = false;
            Map<String, ServiceInstanceInfo> siMap = getMap(sis);
            final long now = currentTimeMillis();
            ServiceInstanceInfo chosen = null;
            String chosenId = null;
            long chosenTimeStamp = 0L;
            int min = Integer.MAX_VALUE;
            long lru = Long.MAX_VALUE, firstStarted = Long.MAX_VALUE;
            long stillLoadingCutoffTime = -1L;
            for (Entry<String, Long> ent : filteredInstances.entrySet()) {
                final String iid = ent.getKey();
                boolean us = false;
                if (!seenSelf && iid.equals(instanceId)) {
                    seenSelf = true;
                    // if balanced then we'll already have selected ourselves
                    // in the invokeModel method if we were a candidate, so skip here
                    if (excludeSelf) {
                        continue;
                    }
                    us = true;
                }
                final ServiceInstanceInfo sii = siMap.get(iid);
                if (sii == null) {
//                  filtered.add(ent); // don't filter not-found ones
                    continue;
                }
                final long loadStarted = ent.getValue();
                if (stillLoadingCutoffTime == -1L) {
                    stillLoadingCutoffTime = now - loadingTimeStats(filtered.modelType).assumeCompletedAfterMillis();
                }
                if (loadStarted < stillLoadingCutoffTime) {
                    // balanced selection logic
                    final ServiceInstance<?> si = (ServiceInstance<?>) sii;
                    final int inuse = us ? localInvokesInFlight.get() : si.getInUseCount();
                    if (inuse > min) {
                        continue;
                    }
                    final long nlu = us ? (preferSelf ? 0L : lastInvokeTime) : si.getLastUsedTime();
                    if (inuse < min) {
                        min = inuse;
                    } else if (nlu >= lru) {
                        continue; // inuse == min
                    }
                    chosen = sii;
                    chosenId = iid;
                    chosenTimeStamp = loadStarted;
                    lru = nlu;
                }
                // ignore new ones if we've already chosen an old one
                else if (min == Integer.MAX_VALUE && loadStarted < firstStarted) {
                    // otherwise choose this one if it's the oldest seen
                    chosen = sii;
                    chosenId = iid;
                    chosenTimeStamp = loadStarted;
                    firstStarted = loadStarted;
                }
            }
            if (chosen != null) {
                // if unbalanced then we might be choosing ourself here,
                // return ABORT_REQUEST (=> ServiceUnavailableException) to indicate this
                if (!excludeSelf && instanceId.equals(chosenId)) {
                    return (T) LoadBalancer.ABORT_REQUEST;
                }
                if (sendDestinationId) {
                    ensureContextMapIsMutable(ThreadContext.getCurrentContext()).put(DEST_INST_ID_KEY, chosenId);
                }
                filtered.add(chosenId, chosenTimeStamp);
            }
            return (T) chosen;
        }
    }

    private Object invokeRemote(BaseModelMeshService.Iface client, Method method, Method remoteMeth, String modelId,
            Object... args) throws Exception {
        if (method == null) {
            return remoteMeth.invoke(client, ObjectArrays.concat(modelId, args));
        }
        return invokeRemoteModel(client, method, remoteMeth, modelId, args);
    }

    // can be overridden
    protected Object invokeRemoteModel(BaseModelMeshService.Iface client, Method method, Method remoteMeth,
            String modelId, Object... args) throws Exception {
        return remoteMeth.invoke(client, ObjectArrays.concat(modelId, args));
    }

    protected Object invokeLocalModel(CacheEntry<?> ce, Method method, Object[] args, String modelId, Boolean isVModel)
            throws InterruptedException, TException {
        Object result = invokeLocalModel(ce, method, false, args);
        // if this is an ensure-loaded request, check-for and trigger a "chained" load if necessary
        if (method == null) {
            triggerChainedLoadIfNecessary(modelId, result, args, ce.getWeight(), null);
        }
        return result;
    }

    private Object invokeLocalModel(CacheEntry<?> ce, Method method, Boolean isVModel, Object[] args)
            throws InterruptedException, TException {

        if (method == null) {
            // if this is a check-status or async ensure-loaded req, don't wait for it
            boolean syncRequest = Boolean.TRUE.equals(args[2]);
            if (!syncRequest && !ce.isDone()) {
                return SI_LOADING;
            }
        } else {
            // if this is a runtime request (actual model invocation), ensure that any loading
            // tasks that we'll wait for have priority in the threadpool queue
            long now = currentTimeMillis();
            ce.upgradePriority(now + 3600_000L, now + 7200_000L); // (2 hours in future)
        }
        Map<String, String> contextMap = ThreadContext.getCurrentContext();
        String vModelId = null; 
        if (isVModel) {
            vModelId = contextMap.get(VMODELID);
        }
        // The future-waiting timeouts should not be needed, request threads are interrupted when their
        // timeouts/deadlines expire, and the model loading thread that it waits for has its own timeout.
        // But we still set a large one as a safeguard (there can be pathalogical cases where model-loading
        // queues are backed-up).
        Object runtime;
        try {
            if (method != null) {
                try {
                    runtime = ce.get(10L, MILLISECONDS);
                } catch (TimeoutException te) {
                    // log cache miss if this is an invocation request (method!=null)
                    // and the model isn't yet loaded (the load might have been
                    // triggered by this request or a parallel one)
                    long safeguardTimeoutMillis = loadTimeoutMs * 5;
                    long beforeNanos = nanoTime() - 10_000_000L;
                    try {
                        runtime = ce.get(safeguardTimeoutMillis, MILLISECONDS);
                    } finally {
                        long delayMillis = msSince(beforeNanos);
                        logger.info("Cache miss for model invocation, held up " + delayMillis + "ms");
                        metrics.logCounterMetric(Metric.CACHE_MISS);
                        metrics.logTimingMetricDuration(Metric.CACHE_MISS_DELAY, delayMillis, false, ce.modelId);
                    }
                }
            } else {
                // method == null: this is an ensure-loaded or get-status req
                // so we are only returning status here
                if (!ce.isDone()) {
                    long safeguardTimeoutMillis = loadTimeoutMs * 5;
                    ce.get(safeguardTimeoutMillis, MILLISECONDS); // wait for it (ensure-loaded case)
                } else {
                    //TODO TBC about exceptions in ensure/status case
                    Throwable failure = ce.finalException();
                    if (failure != null) {
                        throw new ExecutionException(failure);
                    }
                }
                return SI_LOADED;
            }
        } catch (ExecutionException ee) {
            Throwable e = ee.getCause();
            if (e instanceof ModelNotHereException) throw (ModelNotHereException) e;
            if (e instanceof Error) throw (Error) e;
            if (e instanceof ModelLoadException) throw (ModelLoadException) e;

            logger.error("error from getting runtime", e); //TODO this is probably dup/unnecessary

            throw newModelLoadException(null, 0L, e);
        } catch (TimeoutException e) {
            throw newModelLoadException("Timeout waiting for load of model " + ce.modelId, loadTimeoutMs * 5, e);
        }

        // only record invocation if model is actually invoked (i.e. excludes check-status
        // and ensure-loaded cases)
        String batchMult = ThreadContext.getContextEntry(BATCH_COUNT_CXT_KEY);
        //TODO  where to do the reduction?
        int weight = batchMult != null ? Integer.parseInt(batchMult) : 1;

        Code code = null;
        ce.beforeInvoke(weight);
        long beforeNanos = nanoTime();
        try {
            if (!isExternal && (logModelInvocations || logger.isDebugEnabled())) {
                Level level = logModelInvocations ? Level.INFO : Level.DEBUG;
                logger.log(level, "invoking model runtime method " + method.getName() + " of ["
                                  + runtime.getClass().getSimpleName() + "]");
            }

            Object result = invokeLocalMethod(ce.modelId, method, runtime, args);
            code = Code.OK;
            return result;
        } catch (TException te) {
            if (te instanceof ApplierException) {
                String grpcCode = ((ApplierException) te).getGrpcStatusCode();
                code = Code.valueOf(grpcCode);
                if (code == Code.NOT_FOUND) {
                    // runtime unexpectedly reporting model not found - update
                    // cache and registry accordingly
                    deregisterModelAsync(ce.modelId, 0L, ce.loadTimestamp, ce.loadCompleteTimestamp);
                    ce.remove(true);
                    logger.warn("ModelRuntime in instance " + instanceId + " returned unexpected NOT_FOUND for model "
                                + ce.modelId + "; purging from local cache and registration");
                    te = new ModelNotHereException(instanceId, ce.modelId);
                    //TODO trigger cache reconcile task here
                } else if (code == Code.UNAVAILABLE) {
                    // we can't throw a thrift transport exception from the app code,
                    // but this will at least cause the same retry behaviour, just without
                    // circuit-breaking other requests
                    String msg = "ModelRuntime UNAVAILABLE: " + te.getMessage();
                    logger.warn(msg + " for model " + ce.modelId, trimStack(te.getCause(), true));
                    throw trimStack(new InternalException(msg, ""), false);
                }
            }
            throw te;
        } finally {
            long tookNanos = nanoTime() - beforeNanos;
            ce.afterInvoke(weight, tookNanos);
            if (code != null && metrics.isEnabled()) {
                metrics.logRequestMetrics(false, getRequestMethodName(method, args),
                        tookNanos, code, -1, -1, ce.modelId, vModelId);
            }
        }
    }

    protected Object invokeLocalMethod(String modelId, Method method, Object runtime, Object[] args) throws TException {
        try {
            return method.invoke(runtime, args);
        } catch (IllegalAccessException | IllegalArgumentException iae) {
            throw newInternalException("Unexpected exception invoking local model " + modelId, iae);
        } catch (InvocationTargetException ite) {
            Throwable e = ite.getCause();
            Throwables.throwIfInstanceOf(e, TException.class); // this should be a service-defined exception
            Throwables.throwIfInstanceOf(e, Error.class);
            throw newInternalException("Unexpected exception invoking local model", e);
        }
    }

    /*
     * This is invoked on the ensureLoaded path after a loaded/loading model has been touched.
     * It checks the thread context for a "chained load" count, which is how many additional
     * models to verify or load, *not including* the current exclusion list.
     *
     * This is done by triggering another ensureLoaded with the chained load count decremented
     * and this instance appended to the exclusion list.
     */
    protected void triggerChainedLoadIfNecessary(String modelId, Object invokeResult, Object[] args, int weight,
            Map<String, String> contextMap) {
        if (!(invokeResult instanceof StatusInfo)) return;
        Status status = ((StatusInfo) invokeResult).getStatus();
        if (status != Status.LOADING && status != Status.LOADED) return;
        if (!Boolean.TRUE.equals(args[1])) return; // "load" flag must be true
        if (contextMap == null) {
            contextMap = ThreadContext.getCurrentContext();
            if (contextMap == null) return;
        }
        String chainCount = contextMap.get(CHAINED_LOAD_COUNT_KEY);
        if (chainCount == null) return;
        int cci = Integer.parseInt(chainCount); //TODO maybe trap NFE
        if (cci <= 0) return;
        @SuppressWarnings("unchecked")
        List<String> priorExclusions = (List<String>) args[4], toExclude;
        if (priorExclusions != null && !priorExclusions.isEmpty()) {
            toExclude = new ArrayList<>(priorExclusions.size() + 1);
            toExclude.addAll(priorExclusions);
            toExclude.add(instanceId);
        } else {
            toExclude = excludeThisInstance;
        }
        logger.info("Triggering chained load of model " + modelId + "; " + cci + " remaining");
        ensureLoadedInternalAsync(modelId, (Long) args[3], weight, toExclude, cci - 1);
    }

    /* ----------------------------------- global "cache miss" related ------------------------------------------- */

    //check if locations where model has been loaded is greater then the maximum allowed
    private void checkLoadLocationCount(ModelRecord mr, Collection<String> explicitExcludes,
            TException internalFailureSeen) throws TException {
        // throws only ModelLoadException, InternalException or ApplierException with RESOURCE_EXHAUSTED
        int count = 0;
        for (String instId : mr.getInstanceIds().keySet()) {
            if ((explicitExcludes == null || !explicitExcludes.contains(instId)) && instanceInfo.contains(instId)) {
                if (++count >= MAX_LOAD_LOCATIONS) {
                    if (internalFailureSeen != null) {
                        throw internalFailureSeen;
                    }
                    throw newModelLoadException("Failed to invoke model loaded in >=" + count + " locations", 0L, null);
                }
            }
        }
    }

    // check if model load failures have breached the maximum allowed limit
    private void checkLoadFailureCount(ModelRecord mr, ModelLoadException loadFailureSeen)
            throws ModelLoadException {
        Map<String, Long> failedInInstances = mr.getLoadFailedInstanceIds();
        if (!failedInInstances.isEmpty()) {
            int count = 0;
            final long expiryCutoffTime = currentTimeMillis() - IN_USE_LOAD_FAILURE_EXPIRY_MS;
            for (Long failTime : failedInInstances.values()) {
                if (failTime > expiryCutoffTime) {
                    count++; // not yet expired
                }
                if (count >= MAX_LOAD_FAILURES) {
                    if (loadFailureSeen != null) {
                        throw loadFailureSeen;
                    }
                    String failInstance = getMostRecent(failedInInstances);
                    throw new ModelLoadException(mr.getLoadFailureMessage(failInstance),
                            failInstance, 0L, null);
                }
            }
        }
    }

    private static String getMostRecent(Map<String, Long> instances) {
        Entry<String, Long> mostRecent = null;
        for (Entry<String, Long> ent : instances.entrySet()) {
            if (mostRecent == null || mostRecent.getValue() < ent.getValue()) {
                mostRecent = ent;
            }
        }
        return mostRecent != null ? mostRecent.getKey() : null;
    }

    // whether an instance is considered "full" based on remaining/available mem units
    protected boolean isFull(long availableUnits) {
        return availableUnits < minSpaceUnits;
    }

    static final Ordering<Comparable<?>> NULLS_LAST = Ordering.natural().nullsLast();

    final Comparator<Entry<String, InstanceRecord>> PLACEMENT_ORDER = new Comparator<>() {
        @Override
        public int compare(Entry<String, InstanceRecord> e1, Entry<String, InstanceRecord> e2) {
            InstanceRecord ir1 = e1.getValue(), ir2 = e2.getValue();
            if (ir1 == ir2) {
                return e1.getKey().compareTo(e2.getKey());
            }
            boolean sd1 = ir1.isShuttingDown();
            if (sd1 ^ ir2.isShuttingDown()) {
                return sd1 ? 1 : -1;
            }
            long vers1 = ir1.getInstanceVersion(), vers2 = ir2.getInstanceVersion();
            long rem1 = ir1.getRemaining(), rem2 = ir2.getRemaining();
            boolean full1 = isFull(rem1), full2 = isFull(rem2);
            if (vers1 != vers2) {
                // prefer newer version *unless* it's saturated
                if (vers1 > vers2) {
                    if (!full1 || ir1.getLruTime() > minChurnAgeMs * 2) return -1;
                } else /* vers1 < vers2 */
                    if (!full2 || ir2.getLruTime() > minChurnAgeMs * 2) return 1;
            }

            // if only one is full, prefer the other one
            if (full1 ^ full2) return full1 ? 1 : -1;
            if (full1) {
                // if both are full, prefer one containing least recently used
                int oldestDiff = Long.compare(ir1.getLruTime(), ir2.getLruTime());
                if (oldestDiff != 0) return oldestDiff;
            }
            // next prefer one with fewest models by count
            int countDiff = ir1.getCount() - ir2.getCount();
            if (countDiff != 0) return countDiff;
            // next prefer one with largest remaining space
            int remDiff = Long.compare(rem2, rem1);
            if (remDiff != 0) return remDiff;
            if (!full1) {
                // if both are non-full, prefer one containing least recently used
                int oldestDiff = Long.compare(ir1.getLruTime(), ir2.getLruTime());
                if (oldestDiff != 0) return oldestDiff;
            }

            // we must also differentiate based on all these things since this comparator
            // is used to determine equality in the clusterState set.
            // cannot only compare key (instance name), since two entries with the same
            // key will exist in the set briefly during listener updates
            int lip1 = ir1.getLoadingInProgress(), lip2 = ir2.getLoadingInProgress();
            return ComparisonChain.start()
                    .compare(ir2.getLoadingThreads() - lip2, ir1.getLoadingThreads() - lip1)
                    .compare(lip1, lip2)
                    .compare(ir2.getCapacity(), ir1.getCapacity())
                    .compare(ir1.getReqsPerMinute(), ir2.getReqsPerMinute())
                    .compare(e1.getKey(), e2.getKey())
                    .compare(ir1.getLocation(), ir2.getLocation(), NULLS_LAST)
                    .compare(ir1.getZone(), ir2.getZone(), NULLS_LAST)
                    .compare(ir1.getLabels(), ir2.getLabels(), Utils.STRING_ARRAY_COMP)
                    .result();
        }
    };

    //update CacheMissExcludeSet loadTargetFilter
    private static void updateCacheMissExcludeSet(CacheMissExcludeSet loadTargetFilter, String cacheMissExcludeString,
            Collection<String> explicit, ModelRecord mr, long lastUsedTime) {

        if (cacheMissExcludeString != null) {
            Iterables.addAll(loadTargetFilter, COMMA_SPLIT.split(cacheMissExcludeString));
        }
        loadTargetFilter.explicit = explicit;
        loadTargetFilter.updateLoadedAndFailedInstances(mr);
        loadTargetFilter.lastUsedTime = lastUsedTime;
    }

    protected static final class CacheMissExcludeSet extends HashSet<String> {
        final String modelType;
        // whether the source of the originating request is "balanced"
        final boolean favourSelf;
        // the instances in which this model is already loaded or failed
        private Set<String> loaded = Collections.emptySet(), failed = Collections.emptySet();
        // other instances to explicitly exclude
        Collection<String> explicit;

        long lastUsedTime; // this is just to pass through to the LB logic

        public CacheMissExcludeSet(boolean favourSelf, String modelType) {
            super(8);
            this.modelType = modelType;
            this.favourSelf = favourSelf;
        }

        void updateLoadedAndFailedInstances(ModelRecord mr) {
            loaded = mr.getInstanceIds().keySet();
            failed = mr.getLoadFailedInstanceIds().keySet();
        }

        /** @return true if instanceId should be excluded */
        boolean isExcluded(String instanceId) {
            return contains(instanceId) || loaded.contains(instanceId) || failed.contains(instanceId)
                    || (explicit != null && explicit.contains(instanceId));
        }

        @Override
        public String toString() {
            return "LTF" + super.toString() + "[l=" + loaded + ", f=" + failed + ", e=" + explicit + "]";
        }
    }

    protected static final long TWELVE_MIN_MS = MILLISECONDS.convert(12, MINUTES);
    protected static final long ONE_DAY_MS = MILLISECONDS.convert(1, DAYS);
    protected static final long FIVE_DAYS_MS = MILLISECONDS.convert(5, DAYS);

    protected static final ThreadLocal<CacheMissExcludeSet> cacheMissExcludeTl = new ThreadLocal<>();

    class CacheMissForwardingLB extends IdBasedLoadBalancer {

        // Return an iterator which filters the current set of instances based on certain criteria
        private Iterator<Entry<String, InstanceRecord>> filter(Set<String> constrainTo,
                CacheMissExcludeSet exclude, Map<String, ServiceInstanceInfo> siMap,
                ObjectLongMap<String> excludeReplicaSets) {
            return Iterators.filter(clusterState.iterator(), ent -> {
                final String iid = ent.getKey();
                if (constrainTo != null && !constrainTo.contains(iid)
                    || exclude.isExcluded(iid) || !siMap.containsKey(iid)) {
                    return false;
                }
                return excludeReplicaSets.isEmpty() || iid.length() < 7
                       || !excludeReplicaSets.containsKey(iid.substring(0, 6));
            });
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T getNext(Object[] sis, String method, Object[] args) {
            final CacheMissExcludeSet exclude = cacheMissExcludeTl.get();
            final Map<String, ServiceInstanceInfo> siMap = getMap(sis);
            final boolean excludeSelf = exclude.isExcluded(instanceId);
            // if unbalanced then don't favour self
            final boolean favourSelf = exclude.favourSelf;

            // Iterate over the cluster instances which are in order of most to least desirable
            // in terms of placement location (see PLACEMENT_ORDER comparator).
            // Break once we reach an appropriate "distance" from the most desirable instance.
            // Those "seen" form a shortlist for the subsequent random choice.

            // Filter the iterator based on known hard constraints/exclusions
            final Set<String> constrainTo = typeConstraints != null ?
                    typeConstraints.getCandidateInstances(exclude.modelType) : null;

            final ObjectLongMap<String> excludeReplicaSets = upgradeTracker.getLikelyReplacedReplicaSets();
            Iterator<Entry<String, InstanceRecord>> it = filter(constrainTo, exclude, siMap, excludeReplicaSets);
            if (!it.hasNext()) {
                if (excludeReplicaSets.isEmpty()) {
                    return null;
                }
                // If we excluded replicasets that we think are probably being replaced,
                // run the filter again without excluding them. Better to be on the safe side
                // if there are no other placement options
                it = filter(constrainTo, exclude, siMap, ObjectLongMaps.immutable.empty());
                if (!it.hasNext()) {
                    return null; // none suitable found
                }
            }
            Entry<String, InstanceRecord> bestEntry = it.next();
            String bestIid = bestEntry.getKey();
            boolean us = !excludeSelf && instanceId.equals(bestIid);
            // Use freshest details from cache
            InstanceRecord bestInst = us ? getFreshInstanceRecord() : bestEntry.getValue();
            final boolean bestIsFull = isFull(bestInst.getRemaining());

            //TODO consider storing these two lists in a ThreadLocal to avoid re-allocation
            List<String> candidates = new ArrayList<>(siMap.size());
            MutableIntList instReqLoad = new IntArrayList(siMap.size());

            Set<String> prefer = typeConstraints != null ?
                    typeConstraints.getPreferredInstances(exclude.modelType) : null;

            // The "simple case" is if this type has no preference or the "best" instance is
            // preferred (if best is preferred, we never select non-preferred)
            boolean simpleCase = prefer == null || prefer.contains(bestIid);
            if (!simpleCase) {
                // In the non-simple cases we make a copy of the cluster state as we iterate,
                // in preparation for a second pass depending upon if/where preferred instances
                // are encountered relative to the otherwise "best" instance
                List<Entry<String, InstanceRecord>> clusterStateReplay = new ArrayList<>();
                if (!bestIsFull) {
                    // Non-simple case (a) - "best" instance is not full, select based on available capacity.
                    // Designate the first non-full preferred instance found as the "best" and proceed with
                    // simple-case logic, or if no such instance found then rewind and follow
                    // "no preference" simple case logic
                    boolean found = false;
                    while (it.hasNext()) {
                        Entry<String, InstanceRecord> ent = it.next();
                        if (prefer.contains(ent.getKey())) {
                            found = true;
                            bestIid = ent.getKey();
                            bestInst = ent.getValue();
                            us = !us && !excludeSelf && instanceId.equals(bestIid);
                            break;
                        }
                        if (isFull(ent.getValue().getRemaining())) {
                            break;
                        }
                        clusterStateReplay.add(ent);
                    }
                    if (!found) {
                        it = clusterStateReplay.iterator();
                        prefer = null;
                    }
                    simpleCase = true;
                } else {
                    // Non-simple case (b) - "best" instance is full, select based on LRU.
                    // Select only preferred instances with the age distance if there's at least
                    // one, otherwise rewind and fall back to simple "no preference" case
                    long oldest = bestInst.getLruTime();
                    while (it.hasNext()) {
                        Entry<String, InstanceRecord> ent = it.next();
                        String iid = ent.getKey();
                        InstanceRecord curInst = ent.getValue();
                        long diff = curInst.getLruTime() - oldest;
                        // Abort if (absolute and relative) age gap becomes too large in all cases
                        if (diff > 120_000L && diff > age(oldest) / 4) {
                            break;
                        }
                        if (prefer.contains(iid)) {
                            // If preferred instance is encountered, choose _only_ it
                            // plus any other preferred ones within the original age distance
                            us = !us && !excludeSelf && instanceId.equals(iid);
                            if (us && favourSelf) {
                                return null; // this is to indicate that we should be chosen
                            }
                            clusterStateReplay = null;
                            candidates.add(iid);
                            instReqLoad.add(curInst.getReqsPerMinute());
                        } else if (clusterStateReplay != null) {
                            clusterStateReplay.add(ent);
                        }
                    }
                    if (clusterStateReplay != null) {
                        // No preferred instances found, rewind and fall-back to "no preference" logic
                        it = clusterStateReplay.iterator();
                        prefer = null;
                        simpleCase = true;
                    }
                }
            }

            if (simpleCase) {
                if (us && favourSelf) {
                    // If we are a candidate ourselves and the favourSelf flag is set,
                    // choose ourselves immediately (returning ABORT_REQUEST indicates this)
                    return (T) LoadBalancer.ABORT_REQUEST;
                }
                candidates.add(bestIid);
                instReqLoad.add(bestInst.getReqsPerMinute());

                // Simple case, don't need to weigh preferred
                final long oldest = bestInst.getLruTime();
                while (it.hasNext()) {
                    Entry<String, InstanceRecord> ent = it.next();
                    String iid = ent.getKey();
                    // treat preference as required in this case
                    if (prefer != null && !prefer.contains(iid)) {
                        continue;
                    }
                    us = !us && !excludeSelf && instanceId.equals(iid);
                    final InstanceRecord curInst = us ? bestEntry.getValue() : getFreshInstanceRecord();

                    if (bestIsFull) {
                        // If best is full then all must be full due to clusterState ordering
                        long diff = curInst.getLruTime() - oldest;
                        // Keep if LRU age is within certain absolute & relative distances of oldest
                        if (diff > 45_000L && diff > age(oldest) / 10) {
                            break;
                        }
                    } else {
                        long rem = curInst.getRemaining();
                        // Considered "in range" if remaining space is > half best instance remaining
                        // space, and count is either < 10 or < 1.5x best instance count
                        if (isFull(rem) || rem < bestInst.getRemaining() >> 2) {
                            break;
                        }
                        int count = ent.getValue().getCount(), firstCount = bestInst.getCount();
                        if (count >= 10 && count > firstCount + (firstCount >> 2)) {
                            break;
                        }
                    }

                    if (us && favourSelf) {
                        return (T) LoadBalancer.ABORT_REQUEST; // this is to indicate that we should be chosen
                    }

                    candidates.add(iid);
                    instReqLoad.add(curInst.getReqsPerMinute());
                }
            }

            final int ccount = candidates.size();
            if (ccount == 0) {
                return null;
            }

            // use *random* selection from the remaining candidate instances,
            // but first exclude those with high load if this model is
            // currently in use or was used recently

            // note this might be negative in the case of load-triggered
            // scale-up (see scale-up logic in rateTrackingTask())
            final long lastUsedAgo = age(exclude.lastUsedTime);
            String chosenInstId = null;
            if (ccount == 1) {
                chosenInstId = candidates.get(0);
            } else {
                int remainingCount = ccount;
                if (lastUsedAgo < FIVE_DAYS_MS) { // don't filter if last-used more than 5 days ago
                    int minLoad = Math.max(100, instReqLoad.min());
                    int minLoad_1_1 = (int) (1.1 * minLoad), minLoad_1_5 = (int) (1.5 * minLoad);
                    for (int i = 0; i < ccount; i++) {
                        int rpm = instReqLoad.get(i);
                        // exclude under-load instances depending on their req rate relative to the
                        // last-used time of the model to be loaded:
                        if (rpm >= 100 && // (include any with < 100 rpm)
                            // if load-triggered scale-up, exclude any with rate > 1.1x the min
                            (lastUsedAgo < -1000L && rpm > minLoad_1_1
                             // if used in last 5sec, exclude any with rate > 1.5x the min
                             || lastUsedAgo < 5000L && rpm > minLoad_1_5
                             // if used in last 12min, exclude any with rate > 3x the min
                             || lastUsedAgo < TWELVE_MIN_MS && rpm > minLoad * 3
                             // if used in last day, exclude any with rate > 4x the min
                             || lastUsedAgo < ONE_DAY_MS && rpm > minLoad * 4)) {
                            candidates.set(i, null);
                            remainingCount--;
                            if (remainingCount == 1) {
                                break; // shouldn't happen but just in case
                            }
                        }
                    }
                }
                int index = remainingCount == 1 ? 0 : ThreadLocalRandom.current().nextInt(remainingCount);
                for (int i = 0, j = 0; i < ccount; i++) { // pick out the index-th non-null entry
                    if ((chosenInstId = candidates.get(i)) != null && index == j++) {
                        break;
                    }
                }
            }
//          candidates.clear(); //TODO only if reused
            if (!favourSelf && instanceId.equals(chosenInstId)) {
                return (T) LoadBalancer.ABORT_REQUEST; // indicates we are chosen
            }
            boolean exclusions = !exclude.isEmpty();
            if (exclusions || sendDestinationId) {
                Map<String, String> contextMap = ensureContextMapIsMutable(ThreadContext.getCurrentContext());
                if (exclusions) {
                    contextMap.put(CACHE_MISS_EXCLUDES_KEY, COMMA_JOIN.join(exclude));
                }
                if (sendDestinationId) {
                    contextMap.put(DEST_INST_ID_KEY, chosenInstId);
                }
            }
            exclude.add(chosenInstId);
            return (T) siMap.get(chosenInstId);
        }
    }

    // sentinel value returned by loadLocal method to indicate the load was aborted due to
    // a change in the list of recorded loaded-in instances for the model
    private final CacheEntry<?> INSTANCES_CHANGED = new CacheEntry<>(null, 0);

    private static final int INSERTION_WEIGHT = 1;

    private int weightPredictCutoff() {
        return (loadingThreads + (loadingThreads / 3));
    }

    /**
     * @param modelId
     * @param mrh          "pass by reference"
     * @param lastUsedTime
     * @param contextMap
     * @param prioritize
     * @return INSTANCES_CHANGED if model registrations changed
     * => need to reevaluate whether it should still be loaded here,
     * null if model is too old and/or large to fit in the cache
     * @throws Exception
     */
    protected CacheEntry<?> loadLocal(String modelId, ModelRecord[] mrh, long lastUsedTime,
            Map<String, String> contextMap, boolean prioritize) throws Exception { // KV-store exceptions

        //get model record specified for load
        ModelRecord mr = mrh[0];
        if (logger.isDebugEnabled()) {
            logger.debug("loadLocal called for " + modelId + " with lasttime=" + lastUsedTime + " mrinsts="
                         + mr.getInstanceIds());
        }

        long now = currentTimeMillis();

        // prioritize loading if a runtime request will be waiting for this
        long priority = prioritize ? (now + 7200_000L) // (2 hours in future)
                : (lastUsedTime == 0L ? now : lastUsedTime);

        // this should not be the case but is possible due to an earlier (now-fixed)
        // bug which could continue to affect models in already-deployed clusters
        if (lastUsedTime > 0L && lastUsedTime < mr.getLastUsed()) {
            logger.info("Requested last-used time for load of model " + modelId + " older than ModelRecord value ("
                    + readableTime(now - lastUsedTime) + " compared to " + readableTime(now - mr.getLastUsed()) + ")");
            lastUsedTime = mr.getLastUsed();
        }

        // create new entry
        CacheEntry<?> ce = limitModelConcurrency
                ? new MaxConcCacheEntry<>(modelId, mr, now, priority)
                : new CacheEntry<>(modelId, mr, now, priority);
        boolean weCreatedCacheEntry = false, success = false;
        try {
            while (true) {
                CacheEntry<?> existCe;
                if (unloadManager == null) {
                    existCe = runtimeCache.putIfAbsent(modelId, ce, lastUsedTime);
                } else {
                    existCe = unloadManager.insertNewEntry(modelId, ce, lastUsedTime);
                }
                // try to add placeholder entry in local cache
                if (existCe == null) {
                    weCreatedCacheEntry = true;
                    break; // success
                }

                logger.info("Encountered existing cache entry while loading model " + modelId);
                synchronized (existCe) {
                    // A cache entry was already there - most likely that another thread
                    // in this instance is also loading this model (in this same method).
                    // Wait for it to move from NEW to QUEUED state if so, at which point
                    // there should be a corresponding new entry in the ModelRecord
                    // instances map
                    existCe.waitWhileNew(6000L);

                    // re-get latest MR - it's ok to do this from local cache only
                    // since we're only looking for concurrent updates which might
                    // be happening in other threads in this same instance
                    ModelRecord latestMr = registry.get(modelId);
                    if (latestMr == null) {
                        mrh[0] = null;
                        existCe.remove();
                        logger.info("Existing cache entry for model " + modelId + " now gone");
                        return INSTANCES_CHANGED; // ModelNotFoundException will be thrown
                    }

                    if (latestMr.getVersion() != mr.getVersion()) {
                        mrh[0] = latestMr;
                        if (!Objects.equals(latestMr.getInstanceIds(), mr.getInstanceIds())
                            || !Objects.equals(latestMr.getLoadFailedInstanceIds(),
                                mr.getLoadFailedInstanceIds())) {
                            // model registrations changed, re-start main loop
                            logger.info("Registrations changed for " + modelId + ", will reevaluate");
                            return INSTANCES_CHANGED;
                        }
                        mr = latestMr;
                    }

                    int stateNow = existCe.state;
                    if (stateNow > CacheEntry.NEW && stateNow <= CacheEntry.ACTIVE
                        && !existCe.unloadAttemptedRecently()) {
                        // Here state must be QUEUED, WAITING, LOADING, SIZING or ACTIVE.
                        // Odd situation, similar to janitor logic for when a local
                        // cache entry is found without corresponding model record entry,
                        // we just "recycle" the already-loading/loaded one
                        logger.info("Recycling existing entry for " + modelId + "(state=" + ceStateString(stateNow) + ")");
                        ce = existCe;
                        break;
                    }

                    if (stateNow != CacheEntry.REMOVED) {
                        // Here it's either FAILED or still stuck in NEW - consider both of these a failure.
                        // This could happen if the thread we're waiting for fails updating the KV store
                        if (existCe.isFailed()) {
                            assert stateNow == CacheEntry.FAILED;
                            latestMr = handleUnexpectedFailedCacheEntry(existCe, mr);
                            mrh[0] = latestMr;
                            if (!existCe.isRemoved()) {
                                if (latestMr != mr) {
                                    return INSTANCES_CHANGED;
                                }
                                logger.info("Unexpected failed cache entry for model " + modelId
                                        + ", treating as load failure");
                                return existCe;
                            }
                            // else continue to loop now that the entry has been removed
                        } else {
                            // We'll continue to loop in this case for now
                            assert stateNow == CacheEntry.NEW;
                        }
                    }

                    existCe = null;
                    // continue CAS retry loop
                }
            }

            // If placeholder could not be added, it means the new entry was immediately evicted
            // i.e. cache is full, with oldest entry older than this lastUsedTime.
            // Discard the load in this case.
            if (runtimeCache.getQuietly(modelId) != ce) {
                logCacheFallthru(modelId, lastUsedTime, now, runtimeCache.oldestTime(), ce.getWeight());
                return null;
            }

            if (shuttingDown) {
                // don't load if shutting down; waiting threads will see aborted
                if (weCreatedCacheEntry) {
                    ce.remove();
                }
                return ce;
            }

            // predict size of new entry
            int initialSize = 0; // default to loader-predicted size
            String sizeHint = contextMap.get(KNOWN_SIZE_CXT_KEY);
            if (sizeHint != null) {
                initialSize = Integer.parseInt(sizeHint);
            }
            // if this load will be queued, set the initial weight in the cache
            // based on the current average
            else if (loadingCount.get() > weightPredictCutoff()) {
                // so the average is meaningful, only do this if
                // there's >= 20 models already in the cache
                ClusterStats stats = typeSetStats(mr.getType());
                int copyCount = stats.modelCopyCount;
                if (copyCount >= 10) {
                    // negative value indicates that it's an average-based prediction
                    initialSize = -(1 + (int) (stats.totalCapacity - stats.totalFree) / copyCount);
                }
            }
            if (initialSize == 0) {
                initialSize = ce.loaderPredictedWeight();
            }
            int absSize = Math.abs(initialSize);

            // Optimization - abort early if it's going to be evicted immediately.
            // This will catch common case where lastUsed is eldest in cache and the
            // pathological case of predicted size > total capacity, but not other cases.
            // (conditions below ordered roughly by cost of evaluation)
            long oldest = -1L;
            if (weCreatedCacheEntry) {
                long capacity = runtimeCache.capacity();
                if (absSize > capacity || (lastUsedTime > 0 && absSize > capacity - runtimeCache.weightedSize()
                        && lastUsedTime < (oldest = runtimeCache.oldestTime()))) {
                    if (oldest == -1L) {
                        oldest = runtimeCache.oldestTime();
                    }
                    logCacheFallthru(modelId, lastUsedTime, now, oldest, absSize);
                    ce.remove();
                    return null;
                }
            }

            // Try to add current instance to list of registered instances for this model
            while (true) {

                // Add current instance to list of registered instances for this model
                // and if successful then break out of loop
                mr.getInstanceIds().put(instanceId, ce.loadTimestamp);
                // can't be in failed list but remove just in case
                mr.removeLoadFailure(instanceId);
                mr.updateLastUsed(lastUsedTime == 0L ? now : lastUsedTime);
                ModelRecord existMr = registry.conditionalSetAndGet(modelId, mr);
                if (existMr == mr) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("add rec to registry succeeded for "
                                     + modelId + " mrinsts=" + mr.getInstanceIds());
                    }
                    break;
                }
                mrh[0] = existMr;

                // if not successful
                // determine if there are new/updated entries in either of the instance lists

                if (existMr == null) {
                    return INSTANCES_CHANGED; // ModelNotFoundException will be thrown
                }

                // 1. get instances and failed instances associated with model record we are trying to add
                Map<String, Long> iidsBefore = mr.getInstanceIds(), fiidsBefore = mr.getLoadFailedInstanceIds();

                // 2. update model record to latest from registry
                mr = existMr;

                // 3. Revert the "before" instances list - take out the record we just added above
                iidsBefore.remove(instanceId);

                // 4. Get the instances and failed instances from the model record in registry
                Map<String, Long> iidsAfter = mr.getInstanceIds(), fiidsAfter = mr.getLoadFailedInstanceIds();

                // 5. List of iids,fiids for the model record in registry, must not be longer than
                //    iids and fiids for the model record we are trying to update

                // 5.1 If there are no new or updated entries retry immediately
                if (iidsAfter.size() <= iidsBefore.size() && fiidsAfter.size() <= fiidsBefore.size()) {
                    MapDifference<String, Long> diff = Maps.difference(iidsBefore, iidsAfter);
                    if (diff.entriesOnlyOnRight().isEmpty() && diff.entriesDiffering().isEmpty()) {
                        diff = Maps.difference(fiidsBefore, fiidsAfter);
                        if (diff.entriesOnlyOnRight().isEmpty() && diff.entriesDiffering().isEmpty()) {
                            continue;
                        }
                    }
                }
                // 5.2 if there is a change then cancel the new CR and retry (this should be fairly rare)
                logger.info("Add rec to registry conflict for " + modelId + " mrinsts=" + mr.getInstanceIds()
                            + " (not an error)");
                // here, logic in the finally block will be executed
                return INSTANCES_CHANGED;
            }

            // Asynchronously load model as registry addition was successful
            boolean queued = ce.load(initialSize, lastUsedTime); // <- won't throw any exceptions
            if (!queued) {
                return null;
            }

            // update instance record (e.g. maybe went from not-full -> full)
            publishInstanceRecordAsync();

            success = true;
            return ce;
        } catch (Exception e) {
            if (!weCreatedCacheEntry || !(e instanceof ModelNotFoundException)) {
                throw e;
            }
            // Errors here are from a failed KV store update. Fail the cache entry and update this
            // request's temporary copy of the ModelRecord with the failure
            if (ce.preloadFailed(newModelLoadException(
                    "Unable to update KV store prior to model load attempt", KVSTORE_LOAD_FAILURE, e))) {
                mrh[0].addLoadFailure(instanceId, ce.loadCompleteTimestamp,
                        "Unable to update KV store prior to model load attempt: " + e);
                // not actually a success but we don't want to remove the entry yet (in finally block)
                success = true;
            }
            return ce;
        } finally {
            if (weCreatedCacheEntry && !success) {
                // Here ce is the CacheEntry that *we* created AND we're returning *unsuccessfully*
                //
                // In which case we revert the cache entry and "abort" - this means any other
                // waiting threads which happened to have grabbed it from the cache in the meantime
                // will receive a ModelNotHereException if they haven't already received some kind
                // of loading exception (for example in the INSTANCES_CHANGED case)
                ce.remove(); // note: loading won't have started yet
            }
        }
    }

    // This "timeout" value is used as a sentinel to indicate a specific kind of ModelLoadException
    // set in failed cache entries, caused by an error/timeout communicating with the KV store
    // while trying to update the corresponding model record - either prior to loading the model
    // or to correct the record when it's found to be inconsistent with the cache state.
    // These failed entries are intended to avoid exacerbating KV store availability issues by
    // "intercepting" subsequent requests for the model and failing them fast.
    // They "expire" after 30-60sec at which point requests encountering them will re-attempt to
    // retrieve the model record from the KV store and then clean out the failure entry if successful.
    private static final long KVSTORE_LOAD_FAILURE = -2L;

    private ModelRecord handleUnexpectedFailedCacheEntry(CacheEntry<?> ce, ModelRecord mr) {
        // If failure has "expired", re-check registry and remove from cache if appropriate
        Throwable failure = ce.finalException();
        if (failure == null) {
            return mr; // safeguard timeout case (didn't see load fail but timed out waiting for it)
        }
        if (!(failure instanceof ModelLoadException)
            || ((ModelLoadException) failure).getTimeout() != KVSTORE_LOAD_FAILURE) {
            // We assume that this is an expired entry yet to be cleaned up
            if (ce.remove()) {
                logger.info("Removed residual failed cache entry for model " + ce.modelId);
            }
            return mr;
        }

        long failureAge = currentTimeMillis() - ce.loadCompleteTimestamp;
        if (failureAge > 30_000 && failureAge > 30_000
                + ThreadLocalRandom.current().nextLong(30_000)) { // Randomize to avoid thunder

            ModelRecord newMr = registry.get(ce.modelId);
            if (newMr == null ? mr == null : (mr != null && newMr.getVersion() == mr.getVersion())) {
                // First replace the entry with a later-expiring one to block concurrent attempts
                CacheEntry<?> replacement = new CacheEntry<>(ce);
                if (runtimeCache.replaceQuietly(ce.modelId, ce, replacement)) {
                    ce.remove();
                    ce = replacement;
                    try {
                        // this might throw if there are still KV store issues
                        newMr = registry.getStrong(ce.modelId);
                        if (ce.remove()) {
                            logger.info("Removed kv-store failure cache entry for model " + ce.modelId);
                        }
                    } catch (Exception e) {
                        // Cannot verify / still KV store problems
                        logger.warn("Failed to retrieve model record after kv-store failure entry expiry"
                                + " for model " + ce.modelId);
                    }
                }
            }
            return newMr;
        }
        if (mr != null) {
            // Allow failure to propagate (e.g. to invokeLocalModel() after load attempt)
            mr.addLoadFailure(instanceId, ce.loadCompleteTimestamp, failureMessageFromException(failure));
        }
        return mr;
    }

    protected static void logCacheFallthru(String modelId, long lastUsedTime, long now, long lru, int size) {
        if (lastUsedTime == 0L) {
            lastUsedTime = now;
        }
        logger.info("Rejecting load of model " + modelId + " with last-used time of " + lastUsedTime + "ms ("
                    + readableTime(now - lastUsedTime) + " ago) and predicted size " + mb(size * UNIT_SIZE)
                    + " because it is too old and/or large to fit in local cache (LRU is " + readableTime(now - lru)
                    + " ago)");
    }

    protected long getAdjustedCacheCapacity() {
        return unloadManager != null ? unloadManager.getAdjustedCacheCapacity() : runtimeCache.capacity();
    }

    /* --------------------- InstanceRecord (local cache state) publishing --------------------------------- */

    protected InstanceRecord getFreshInstanceRecord() {
        long oldest = runtimeCache.oldestTime();
        long cap = runtimeCache.capacity(), used = runtimeCache.weightedSize();
        int count = runtimeCache.size(); //TODO maybe don't get every time
        if (unloadManager != null) {
            // remove unloading buffer weight from published values
            int weight = unloadManager.getUnloadBufferWeight();
            cap -= weight;
            used -= weight;
            count--;
        }
        if (oldest == -1L) {
            oldest = Long.MAX_VALUE;
        }
        int lThreads = loadingThreads, lInProg = loadingCount.get();
        return new InstanceRecord(instanceStartTime, longVersion, instanceLocation, instanceZone,
                instanceLabels, oldest, count, cap, used, lThreads, lInProg, shuttingDown);
    }

    protected volatile long lastPublished;
    protected final Lock publishLock = new ReentrantLock();

    protected void publishInstanceRecord(boolean force, boolean preShutdown) throws Exception {
        boolean isShuttingDown = shuttingDown;
        if (!preShutdown ? publishLock.tryLock() : publishLock.tryLock(6L, SECONDS)) {
            try {
                long now = currentTimeMillis(), lastDone = now - lastPublished;
                // don't even check if last publish was < 2sec ago
                if (!preShutdown && (lastDone < INSTANCE_REC_PUBLISH_MIN_PERIOD_MS
                        || (!force && lastDone < (INSTANCE_REC_PUBLISH_FREQ_MS - 1000L)))) {
                    return;
                }

                boolean old = lastDone > (INSTANCE_REC_PUBLISH_FREQ_MS * 4L);

                long oldest = runtimeCache.oldestTime();
                long cap = runtimeCache.capacity(), used = runtimeCache.weightedSize();
                int count = runtimeCache.size(); //TODO maybe don't get every time
                int unloadBufferWeight = -1, totalUnloadingWeight = -1;
                long totalCacheOccupancy = -1;
                if (unloadManager != null) {
                    runtimeCache.getEvictionLock().lock();
                    try {
                        unloadBufferWeight = unloadManager.getUnloadBufferWeight();
                        totalUnloadingWeight = unloadManager.getTotalUnloadingWeight();
                        totalCacheOccupancy = unloadManager.getTotalModelCacheOccupancy();
                    } finally {
                        runtimeCache.getEvictionLock().unlock();;
                    }
                    // remove unloading buffer weight from published values
                    cap -= unloadBufferWeight;
                    used -= unloadBufferWeight;
                    count--;
                }
                if (oldest == -1L) {
                    oldest = Long.MAX_VALUE;
                }

                int loadThreads = loadingThreads, loadInProg = loadingCount.get();
                int rpms = invokeCounter.getBusyness();

                InstanceRecord curRec = instanceInfo.getOrStrongIfAbsent(instanceId);
                while (true) {
                    if (curRec == null) {
                        if (shuttingDown) {
                            return;
                        }
                        curRec = new InstanceRecord(instanceStartTime, longVersion,
                                instanceLocation, instanceZone, instanceLabels, oldest,
                                count, cap, used, loadThreads, loadInProg, shuttingDown);
                    } else {
                        // if changes are within thresholds, don't update (reduce traffic)
                        if (!old) {
                            long diff;
                            if (curRec.isShuttingDown() == isShuttingDown
                                && Math.abs(curRec.getCapacity() - cap) < cap / 50L
                                && (diff = Math.abs(curRec.getLruTime() - oldest)) < 20_000L
                                && (curRec.getLruTime() == Long.MAX_VALUE
                                    || diff < (now - curRec.getLruTime()) / 16)
                                && (diff = Math.abs(curRec.getCount() - count)) < 10
                                && (curRec.getCount() == 0 ? count == 0 :
                                    (diff * 100L) / curRec.getCount() < 15)
                                && (curRec.getUsed() == 0 ? used == 0 :
                                    (Math.abs(curRec.getUsed() - used) * 100) / curRec.getUsed() < 20)
                                && isFull(curRec.getRemaining()) == isFull(Math.max(0L, cap - used))
                                && curRec.getLoadingThreads() == loadThreads
                                && !loadingChange(curRec, loadInProg)
                                && !loadChange(curRec.getReqsPerMinute(), rpms)) {
                                return;
                            }
                        }
                        // if not updated for a while, update if *any* change
                        else if (curRec.isShuttingDown() == isShuttingDown
                                 && curRec.getCapacity() == cap && curRec.getCount() == count
                                 && curRec.getLruTime() == oldest && curRec.getUsed() == used
                                 && curRec.getLoadingThreads() == loadThreads
                                 && curRec.getLoadingInProgress() == loadInProg
                                 && curRec.getReqsPerMinute() == rpms) {
                            return;
                        }

                        curRec.setShuttingDown(isShuttingDown);
                        curRec.setCapacity(cap);
                        curRec.setLruTime(oldest);
                        curRec.setCount(count);
                        curRec.setUsed(used);
                        curRec.setLoadingThreads(loadThreads);
                        curRec.setLoadingInProgress(loadInProg);
                        curRec.setReqsPerMinute(rpms);
                    }

                    curRec.setActionableUpdate();

                    SessionNode ourNode = myNode;
                    if (ourNode != null) {
                        ourNode.setDefaultValue(INST_REC_SERIALIZER.serialize(curRec));
                    }
                    long sessionId = kvUtilsFactory.getSessionId();
                    if (sessionId == 0L) {
                        return; // session lease not yet established
                    }

                    // technically a "conditional" set shouldn't be required here
                    // since we should be the only ones modifying our own record
                    InstanceRecord existRec = instanceInfo.conditionalSetAndGet(instanceId, curRec, sessionId);
                    if (existRec == curRec) {
                        curRec.setActionableUpdate();
                        String message = "Published new instance record: " + curRec;
                        if (unloadBufferWeight != -1) {
                            // Also log some internal values to help identify cache accounting anomalies
                            message += ", UBW=" + unloadBufferWeight + ", TUW=" + totalUnloadingWeight
                                    + ", TCO=" + totalCacheOccupancy;
                        }
                        logger.info(message);
                        lastPublished = now;
                        // our own record in clusterState will subsequently be
                        // updated via the instanceInfo listener.
                        metrics.logInstanceStats(curRec);
                        return;
                    }
                    curRec = existRec;
                    isShuttingDown = shuttingDown;
                }
            } catch (Exception e) {
                // suppress exceptions if shutting down; instanceTable may have been closed
                if (!shuttingDown) {
                    throw e;
                }
            } finally {
                publishLock.unlock();
            }
        }
    }

    protected void publishInstanceRecordAsync() {
        if (!shuttingDown) {
            taskPool.execute(() -> {
                try {
                    publishInstanceRecord(true, false);
                } catch (Exception e) {
                    logger.error("Error publishing instance record", e);
                }
            });
        }
    }

    // determines whether a change in # in-progress loadings is "significant"
    protected static boolean loadingChange(InstanceRecord curRec, int loadInProg) {
        int curInProg = curRec.getLoadingInProgress();
        if (loadInProg == curInProg) return false;
        if (loadInProg == 0 ^ curInProg == 0) return true;
        int lThreads = curRec.getLoadingThreads();
        if (loadInProg <= lThreads ^ curInProg <= lThreads) return true;
        return Math.abs(loadInProg - curInProg) >= 3;
    }

    // determines whether a change in req rate is "significant"
    protected static boolean loadChange(int curRecRpms, int rpms) {
        int diff = Math.abs(curRecRpms - rpms);
        return diff >= 100 || (curRecRpms == 0
                ? rpms != 0 : (100 * diff) / curRecRpms > 10);
    }

    protected List<String> getCacheState() {
        List<String> info = new ArrayList<>(32);
        ServiceDeploymentInfo sdi = getInstanceDeploymentInfo();
        info.add("CACHESTATE: This instance: " + instanceId + " ("
                 + sdi.getPublicAddress() + ":" + sdi.getPublicPort() + ")");
        long time = currentTimeMillis();
        info.add("CACHESTATE: Local clock time: " + time + " (" + new Date(time) + ")");
        if (!limitModelConcurrency) {
            info.add("CACHESTATE: Total model invocations since startup: " + invokeCounter.getTotalReqCount());
        } else {
            info.add(String.format("CACHESTATE: Total invocation processing time since startup: %.2fsec",
                    invokeCounter.getTotalReqCount() / 100_000.0));
        }
        Map<String, TimeStats> timeStatsMap = loadTimeStats;
        info.add("CACHESTATE: " + timeStatsMap.size() + " model types seen: " + timeStatsMap.keySet());
        for (Entry<String, TimeStats> ent : timeStatsMap.entrySet()) {
            TimeStats stats = ent.getValue();
            info.add("CACHESTATE: Model loading stats for type " + ent.getKey() + ": count=" + stats.count() + " mean="
                     + stats.mean() + "ms assume-loaded-after=" + stats.assumeCompletedAfterMillis() + "ms");
        }
        Map<String, Long> cacheEntries = runtimeCache.descendingLruMap();
        info.add("CACHESTATE: Model cache summary: " + getFreshInstanceRecord());
        info.add("CACHESTATE: -------------------------------------------------------------------");
        info.add("CACHESTATE: Descending entries:");
        long now = currentTimeMillis(), timeSinceLastRateCheck = now - lastCheckTime;
        for (Entry<String, Long> ent : cacheEntries.entrySet()) {
            CacheEntry<?> ce = runtimeCache.getQuietly(ent.getKey());
            if (ce == null) {
                continue; // deleted
            }
            long invokeCount = ce.getTotalInvocationCount(), delta = invokeCount - ce.lastInvokeCount;
            String rpm;
            if (timeSinceLastRateCheck < 1000L) rpm = "?";
            else rpm = String.valueOf(delta == 0L ? 0L : (60_000L * delta) / timeSinceLastRateCheck);
            info.add("CACHESTATE: " + ent.getKey() + " " + ce.stateString() + " weight=" + ce.getWeight()
                    + (ent.getValue() != Long.MAX_VALUE
                    ? " last used " + readableTime(now - ent.getValue()) + " ago (" + ent.getValue() + ") "
                    + invokeCount + " invocations, current load " + rpm + "rpm"
                    : ""));
        }
        info.add("");
        info.add("CLUSTSTATE: Cluster state summary: " + clusterStats);
        info.add("CLUSTSTATE: -------------------------------------------------------------------");
        String leaderId;
        try {
            leaderId = leaderLatch.getLeaderId();
        } catch (Exception e) {
            leaderId = "[exception obtaining: " + e.getClass().getName() + ": " + e.getMessage() + "]";
        }
        info.add("CLUSTSTATE: Leader instance is " + leaderId);
        info.add("CLUSTSTATE: Instances in placement desirability order ( (F) => \"full\" ):");
        for (Entry<String, InstanceRecord> ent : clusterState) {
            boolean full = isFull(ent.getValue().getRemaining());
            info.add("CLUSTSTATE: " + ent.getKey() + ": " + (full ? "(F) " : "") + ent.getValue());
        }
        return info;
    }

    /* ------------------------- scheduled background tasks / leader election ------------------------------ */

    // read/write in rateTrackingTask, read only in janitorTask
    volatile long lastCheckTime = currentTimeMillis();

    /*
     * Runs in every instance, checks request rates for models in our cache, and triggers
     * model scale-out beyond 2 copies when necessary and possible
     */
    private Runnable rateTrackingTask() {
        return new Runnable() {
            final int secondCopyMaxAgeIters = (int) ((SECOND_COPY_MAX_AGE_SECS * 1000L) / RATE_CHECK_INTERVAL_MS);
            final int secondCopyMinAgeIters = (int) ((SECOND_COPY_MIN_AGE_SECS * 1000L) / RATE_CHECK_INTERVAL_MS);

            // Minimum LRU when cache is close to full for permitting redundant model copies.
            // This aims to avoid flip-flopping by ensuring that the scale-down lastUsed cutoff
            // (10% of LRU) is at least 3x the SECOND_COPY_MAX_AGE_SECS scale-up threshold.
            // Impose an absolute minimum of 6 hours.
            final int secondCopyLruThresholdMillis = Math.max(SECOND_COPY_MAX_AGE_SECS * 3 * 10 * 1000, 6 * 3600_000);

            // this is incremented each iteration (~ every RATE_CHECK_INTERVAL_MS)
            int iterationCounter;

            // applies only to time-tracking mode, updated after each non-empty iteration
            double averageModelParallelism = 1.0;

            @Override
            public void run() {
                Thread curThread = Thread.currentThread();
                String threadNameBefore = setThreadName(curThread, "rate-track-task");
                try {
                    final long lastTime = lastCheckTime, now = currentTimeMillis();
                    final long timeDelta = now - lastTime; // should be ~= RATE_CHECK_INTERVAL_MS

                    // skip if too soon - i think this can happen if task executions
                    // get backed up in the taskPool
                    if (timeDelta * 5L < RATE_CHECK_INTERVAL_MS * 3L) {
                        return;
                    }

                    // Determine the upper and lower bounds (inclusive) in terms of
                    // iteration number between which a prior use of a single-copy model
                    // will trigger a second copy to be loaded.
                    final int lower = iterationCounter - secondCopyMaxAgeIters;
                    final int upper = iterationCounter - secondCopyMinAgeIters;

                    try {
                        int instCount = clusterStats.instanceCount;
                        if (instCount < 2) {
                            return; // can't scale if there are no other instances
                        }

                        final long before = logger.isDebugEnabled() ? nanoTime() : 0L;

                        Map<String, CacheEntry<?>> usedSinceLastRun = runtimeCache.descendingMapWithCutoff(lastTime);
                        if (unloadManager != null) {
                            unloadManager.removeUnloadBufferEntry(usedSinceLastRun);
                        }
                        if (usedSinceLastRun.isEmpty()) {
                            return; // no invocations since last check
                        }

                        // last-used timestamp to assign to newly triggered copy loads.
                        // Set to 20sec in the *future* so that the logic in CacheMissForwardingLB
                        // will have less placement tolerance w.r.t. how loaded the instances are
                        final long newCopiesTimestamp = now + 20_000L;

                        boolean latencyBased = limitModelConcurrency;
                        int scaleUpRpms = 0, heavyRpms = 0;
                        if (!latencyBased) {
                            scaleUpRpms = scaleUpRpmThreshold;
                            heavyRpms = (scaleUpRpms * 3) / 4;
                        }
                        String modelId = null;
                        Set<String> excludeSet = null; // instances to exclude because they are already highly loaded
                        int modelParallelismSum = 0;
                        for (Entry<String, CacheEntry<?>> ent : usedSinceLastRun.entrySet()) {
                            try {
                                modelId = ent.getKey(); // used in catch block
                                CacheEntry<?> ce = ent.getValue();
                                final long count = ce.getAndResetIntervalCount(); // Always reset
                                ClusterStats clusterStats = typeSetStats(ce.modelInfo.serviceType);
                                int suitableInstCount = instCount;
                                if (typeConstraints != null) {
                                    // Check if this model type is constrained to a subset of instances
                                    // and skip if that subset comprises only one instance (us)
                                    suitableInstCount = clusterStats.instanceCount;
                                    if (suitableInstCount < 2) {
                                        continue;
                                    }
                                }

                                if (latencyBased) {
                                    MaxConcCacheEntry<?> mcce = (MaxConcCacheEntry<?>) ce;
                                    scaleUpRpms = mcce.getRpmScaleThreshold(true);
                                    heavyRpms = (scaleUpRpms * 3) / 4;
                                    modelParallelismSum += mcce.maxConc;
                                }
                                int rpm = (int) ((count * 60_000L) / timeDelta);
                                //System.out.println("DEBUG: rateTrack "+modelId+" threshold="+scaleUpRpms+" current="+rpm);
                                if (rpm > heavyRpms) ce.setLastHeavyTime(now);

                                ModelRecord mr = registry.get(modelId);
                                if (mr == null) continue; // not found in registry (not expected)
                                // note, it's not possible for inst to be in both loaded + failed lists
                                int loadedCount = mr.getInstanceIds().size();
                                if (loadedCount == 0) {
                                    continue; // unexpected - likely mid unload
                                }
                                int failedCount = mr.getLoadFailedInstanceIds().size();

                                int candidateInstCount = suitableInstCount - (loadedCount + failedCount);
                                if (candidateInstCount <= 0) continue; // no space to load new one

                                // For 1->2 copies, scale-up can also be triggered by a pattern of recent usage
                                // See explanation of CacheEntry#usageSlices
                                if (loadedCount == 1) {
                                    // assert mr.getInstanceIds().containsKey(instanceId);

                                    int i1 = ce.earlierUseIteration, i2 = ce.lastUsedIteration;
                                    // invariants: lower < upper, i1 <= i2
                                    if (logger.isDebugEnabled()) {
                                        logger.debug("Second copy trigger evaluation for model " + modelId
                                            + ": target range [" + lower + ", " + upper + "], I1="
                                                + i1 + ", I2=" + i2 + ", curIteration=" + iterationCounter);
                                    }
                                    boolean i1inRange = false, i2inRange = false;
                                    if (i2 >= lower && i1 <= upper) {
                                        i1inRange = i1 >= lower;
                                        i2inRange = i2 <= upper;
                                    }
                                    if (i2inRange || !i1inRange) {
                                        ce.earlierUseIteration = i2;
                                    }
                                    ce.lastUsedIteration = iterationCounter;

                                    if (i1inRange || i2inRange) {
                                        // Model was used within the target range [MIN_AGE, MAX_AGE] iterations ago
                                        // so trigger loading of a second copy

                                        // Don't do it if > 90% full and cache is younger than secondCopyLruThresholdMillis
                                        if ((10 * clusterStats.totalFree) / clusterStats.totalCapacity >= 1
                                                || (now - clusterStats.globalLru) > secondCopyLruThresholdMillis) {
                                            logger.info("Attempting to add second copy of model " + modelId
                                                    + " in another instance since \"regular\" usage was detected");
                                            ensureLoadedInternalAsync(modelId, lastTime, ce.getWeight(), excludeThisInstance, 0);
                                            continue;
                                        }
                                    }
                                }

                                //TODO *maybe* tweak this more based on cluster state (other rpms)
                                if (rpm < scaleUpRpms) continue; // load not high enough

                                // skip if a prior load was too recent to gauge new load requirement
                                // - the measured req load won't fully take into account their serving contribution.
                                // (unless it was our load, in which case it shouldn't be an overestimate)
                                long recentLoadCutoff = now - (timeDelta + RATE_CHECK_INTERVAL_MS
                                        + 2L * loadingTimeStats(mr.getType()).assumeCompletedAfterMillis());
                                if (loadedSince(mr, recentLoadCutoff, instanceId)) continue;

                                Set<String> ourExcludeSet;
                                if (excludeSet == null) {
                                    excludeSet = getExcludeSet(); // (construct lazily)
                                }
                                int excludedCount = excludeSet.size();
                                if (excludedCount != 0) {
                                    for (String iid : excludeSet) {
                                        if (!mr.getInstanceIds().containsKey(iid)
                                                && !mr.loadFailedInInstance(iid)) {
                                            candidateInstCount--;
                                        }
                                    }
                                    candidateInstCount -= excludedCount;
                                    if (candidateInstCount <= 0) {
                                        continue; // no *suitable* candidates
                                    }
                                    ourExcludeSet = Sets.union(excludeSet, mr.getInstanceIds().keySet());
                                } else {
                                    ourExcludeSet = mr.getInstanceIds().keySet();
                                }

                                int copiesToLoad = Math.min(rpm / scaleUpRpms, candidateInstCount);
                                // cap # copies to load in one go at max(2, 33% of cluster size)
                                if (copiesToLoad > 2) {
                                    copiesToLoad = Math.min(copiesToLoad, suitableInstCount / 3);
                                }

                                List<String> toExclude = new ArrayList<>(ourExcludeSet);
                                logger.info("Triggering scale-up of model " + modelId + " by " + copiesToLoad
                                        + ", from " + loadedCount + " -> " + (loadedCount + copiesToLoad) + " copies"
                                        + (failedCount > 0 ? " (also has " + failedCount + " failure records)" : "")
                                        + ". RPM over last " + timeDelta + "ms was " + rpm
                                        + ", current threshold is " + scaleUpRpms
                                        + (excludedCount > 0 ? " (" + excludedCount + " instances excluded)" : ""));
                                ensureLoadedInternalAsync(modelId, newCopiesTimestamp, ce.getWeight(), toExclude,
                                        copiesToLoad - 1);
                            } catch (RuntimeException e) {
                                // shouldn't happen - but just in case continue loop
                                logger.error("Unexpected runtime exception while processing request rate for model"
                                        + (modelId != null ? " " + modelId : ""), e);
                            } finally {
                                modelId = null;
                            }
                        }
                        if (latencyBased) {
                            averageModelParallelism = Math.max(1.0,
                                    ((double) modelParallelismSum) / usedSinceLastRun.size());
                        }
                        if (before != 0L) {
                            logger.debug("rateTrackingTask took " + msSince(before) + "ms, processed "
                                    + usedSinceLastRun.size() + " models");
                        }

                    } finally {
                        // only set this if we've reset the model counts
                        lastCheckTime = now;
                        iterationCounter++;
                    }
                } finally {
                    curThread.setName(threadNameBefore);
                }
            }

            // instances to exclude from scale-out because they are already highly loaded
            private Set<String> getExcludeSet() {
                int scaleUpRpms = limitModelConcurrency ? (int) (900.0 * averageModelParallelism) : scaleUpRpmThreshold;
                int ourRpm = invokeCounter.getBusyness();
                // the tolerance is increased here if our own rpm value is significantly
                // above the threshold - doesn't make sense to exclude ones that are much less
                // loaded than this one
                int maxRpm = Math.max(scaleUpRpms * 4, ourRpm - 2 * scaleUpRpms);
                Set<String> excludeSet = null;
                for (Entry<String, InstanceRecord> ent : clusterState) {
                    String iid = ent.getKey();
                    if (iid.equals(instanceId)) {
                        continue;
                    }
                    if (ent.getValue().getReqsPerMinute() > maxRpm) {
                        if (excludeSet == null) {
                            excludeSet = new TreeSet<>();
                        }
                        excludeSet.add(iid);
                    }
                }
                return excludeSet != null ? excludeSet : Collections.emptySet();
            }
        };
    }

    protected static boolean loadedSince(ModelRecord mr, long recentLoadCutoff, String ignoreInstance) {
        for (Entry<String, Long> entry : mr.getInstanceIds().entrySet()) {
            if (ignoreInstance != null && ignoreInstance.equals(entry.getKey())) {
                continue;
            }
            Long loadStartTime = entry.getValue();
            if (loadStartTime != null && loadStartTime > recentLoadCutoff) {
                return true;
            }
        }
        return false;
    }

    /*
     * Janitor task runs in every instance, deals with this instance's cache and registered models only
     */
    private Runnable janitorTask() {
        return new Runnable() {
            @Override
            public void run() {
                if (shuttingDown) {
                    return;
                }
                Thread curThread = Thread.currentThread();
                String threadNameBefore = setThreadName(curThread, "janitor-task");
                try {
                    if (!verifyKvStoreConnection()) {
                        logger.warn("Skipping run of janitor task due to failed kv-store conn check");
                        return;
                    }

                    long before = nanoTime();
                    Map<String, CacheEntry<?>> cacheEntries = runtimeCache.descendingMap();
                    if (unloadManager != null) {
                        unloadManager.removeUnloadBufferEntry(cacheEntries);
                    }
                    int count = cacheEntries.size();
                    if (count > 0) {
                        boolean cacheChanged = false;
                        long now = currentTimeMillis();
                        long lastLastUsed = Long.MAX_VALUE;
                        // loop over local cache most recently used to least recent
                        for (Entry<String, CacheEntry<?>> ent : cacheEntries.entrySet()) {
                            try {
                                CacheEntry<?> ce = ent.getValue();
                                if (!ce.isDone()) {
                                    continue; // skip if still loading
                                }
                                String modelId = ent.getKey();
                                final long lastUsed = runtimeCache.getLastUsedTime(modelId);
                                if (lastUsed <= 0L) {
                                    continue; // no longer in cache
                                }
                                if (lastUsed > lastLastUsed) {
                                    logger.warn("Encountered out-of-order cache entry timestamp: " + modelId
                                                + " lastUsed is " + lastUsed + " > " + lastLastUsed);
                                }
                                lastLastUsed = lastUsed;

                                ModelRecord mr = registry.get(modelId);

                                if (lastUsed == Long.MAX_VALUE) {
                                    // Cache entries here should not have a Long.MAX_VALUE lastUsed value but this has been observed
                                    // in some environments. Repair/log here so that the entry is not pinned in the cache.
                                    if (runtimeCache.forceSetLastUsedTime(modelId, now - (LASTUSED_AGE_ON_ADD_MS * 3L))) {
                                        logger.warn("Force-updated unexpected Long.MAX_VALUE lastUsed time in cache for model " + modelId);
                                        // Also update in registry if needed
                                        repairLastUsedTimeIfNeeded(modelId, mr);
                                    }
                                    return;
                                }

                                // skip if new or recently used
                                final boolean newOrUsedRecently = now - lastUsed < LOCAL_JANITOR_FREQ_SECS * 2000L
                                                                                   + loadTimeoutMs;
                                if (newOrUsedRecently) {
                                    if (mr != null) {
                                        updateLastUsedTimeInRegistryIfStale(modelId, mr, lastUsed);
                                    }
                                    continue;
                                }
                                final boolean failed = ce.isFailed();
                                if (mr == null || !(failed ? mr.getLoadFailedInstanceIds() : mr.getInstanceIds())
                                        .containsKey(instanceId)) {
                                    mr = registry.getStrong(modelId);
                                }

                                while (true) {
                                    Long regLoadTimestamp = null;
                                    if (mr != null) {
                                        Map<String, Long> insts = failed ? mr.getLoadFailedInstanceIds()
                                                : mr.getInstanceIds();
                                        long localTimestamp = failed ? ce.loadCompleteTimestamp : ce.loadTimestamp;
                                        Long regTimestamp = insts.get(instanceId);
                                        if (regTimestamp != null && regTimestamp == localTimestamp) {
                                            updateLastUsedTimeInRegistryIfStale(modelId, mr, lastUsed);
                                            break; // all good, continue outer loop
                                        }
                                        if (!failed) {
                                            regLoadTimestamp = regTimestamp;
                                        }
                                    }
                                    if (shuttingDown) {
                                        return;
                                    }
                                    // if model no longer exists at all in registry
                                    // OR it's not registered to our instance and we just have a failure record,
                                    // remove it from the local cache.
                                    if (mr == null || ce.state < CacheEntry.LOADING || ce.state > CacheEntry.ACTIVE // includes failed
                                        || ce.unloadAttemptedRecently()) {
                                        if (ce.remove()) {
                                            logger.info("Janitor removed" + (failed ? " *failed*" : "") + " model "
                                                        + modelId + " from cache because not present in registry");
                                            cacheChanged = true;
                                        }
                                        break; // continue outer loop
                                    }
                                    // else it's properly loaded/loading and just not registered to our instance,
                                    // OR registered with a different load timestamp (unexpected)
                                    // - leave in the cache and update the registry
                                    mr.getInstanceIds().put(instanceId, ce.loadTimestamp);
                                    mr.removeLoadFailure(instanceId); // just in case
                                    mr.updateLastUsed(lastUsed);
                                    ModelRecord existMr = registry.conditionalSetAndGet(modelId, mr);
                                    if (existMr == mr) {
                                        if (regLoadTimestamp == null) {
                                            logger.info("Loaded model " + modelId + " found in local instance but not"
                                                        + " in registry, now added to registry with load time "
                                                        + ce.loadTimestamp);
                                        } else {
                                            logger.warn("Load timestamp of model " + modelId + " mismatch (cache="
                                                        + ce.loadTimestamp + ", registry=" + regLoadTimestamp
                                                        + "); updated registry.");
                                        }
                                        break; // continue outer loop
                                    }
                                    mr = existMr;
                                }
                            } catch (Exception e) {
                                logger.error("Janitor exception while processing cache entry for model " + ent.getKey(), e);
                            }
                        }
                        if (cacheChanged) {
                            publishInstanceRecordAsync();
                        }
                        // probably demote this to debug later, or do based on some time threshold
                        logger.info("Janitor cache pruning task took " + msSince(before)
                                    + "ms for " + count + " entries");
                    }

                    if (shuttingDown) {
                        return;
                    }
                    before = nanoTime();
                    // find references to this instance in the registry and prune where necessary
                    String modelId = null;
                    int i = 0, j = 0;
                    SortedSet<Entry<Object[], Long>> scaleCopiesCandidates = new TreeSet<>(VALUE_COMP);
                    long now = currentTimeMillis();
                    registryLoop:
                    for (Entry<String, ModelRecord> ent : registry.noThrowIterable()) {
                        try {
                            i++;
                            modelId = ent.getKey();
                            ModelRecord mr = ent.getValue();

                            // this loop is just for the conditional updates
                            while (true) {
                                boolean loaded = mr.getInstanceIds().containsKey(instanceId);
                                Long failedTime = mr.getLoadFailedInstanceIds().get(instanceId);
                                if (!loaded && failedTime == null) {
                                    continue registryLoop;
                                }

                                CacheEntry<?> ce = cacheEntries.get(modelId);
                                if (ce == null) {
                                    ce = runtimeCache.getQuietly(modelId);
                                }
                                long lastUsed = -2L;
                                boolean remLoaded = loaded && (ce == null || ce.isFailed());
                                boolean remFailed = false;
                                if (failedTime != null) {
                                    if (ce != null && !ce.isFailed()) {
                                        remFailed = true;
                                    } else {
                                        lastUsed = ce != null ? runtimeCache.getLastUsedTime(modelId) : -1L;
                                        // Use shorter expiry age if model was used in last 3 minutes
                                        final long expiryAge = (lastUsed > 0 && (now - lastUsed) < SHORT_EXPIRY_RECENT_USE_TIME_MS)
                                                ? IN_USE_LOAD_FAILURE_EXPIRY_MS : LOAD_FAILURE_EXPIRY_MS;
                                        if (now - failedTime > expiryAge) {
                                            remFailed = true;
                                        }
                                    }
                                }
                                if (remLoaded || remFailed) {
                                    if (shuttingDown) {
                                        return;
                                    }
                                    // we need to remove ourselves from the loaded and/or failed lists
                                    if (remLoaded) {
                                        mr.getInstanceIds().remove(instanceId);
                                        mr.updateLastUnloadTime();
                                    }
                                    if (remFailed) {
                                        mr.removeLoadFailure(instanceId);
                                    }
                                    if (ce != null) {
                                        if (lastUsed == -2) {
                                            lastUsed = runtimeCache.getLastUsedTime(modelId);
                                        }
                                        if (lastUsed > 0L) {
                                            mr.updateLastUsed(lastUsed);
                                        }
                                    }
                                    ModelRecord existMr = registry.conditionalSetAndGet(modelId, mr);
                                    if (existMr != mr) {
                                        mr = existMr;
                                        continue;
                                    }
                                    if (remLoaded) {
                                        logger.info("Janitor removed registration for model"
                                                + " not found in local cache: " + modelId);
                                    }
                                    if (remFailed) {
                                        logger.info("Janitor removed expired failure record with timestamp "
                                                    + failedTime + " for model: " + modelId);
                                    }
                                }
                                j++;
                                if (remFailed && ce != null && ce.isFailed()) {
                                    // Also remove expired failure records from local cache
                                    ce.remove();
                                } else if (loaded && !remLoaded) {
                                    if (lastUsed <= 0L) {
                                        lastUsed = runtimeCache.getLastUsedTime(modelId);
                                    }
                                    if (lastUsed > 0L) {
                                        scaleCopiesCandidates.add(Maps.immutableEntry(
                                                new Object[] { modelId, mr, ce }, lastUsed));
                                    }
                                    // else no longer exists in cache
                                }
                                break;
                            }
                        } catch (Exception e) {
                            logger.error("Janitor exception while processing registry entry"
                                         + (modelId != null ? " for model " + modelId : ""), e);
                        }
                    }

                    if (!scaleCopiesCandidates.isEmpty()) {
                        if (shuttingDown) {
                            return;
                        }
                        modelId = null;
                        // this is a limit on the cumulative weight of models which are removed in this run
                        // - can't be more than 5% of the local cache capacity
                        long maxWeightToRemove = getAdjustedCacheCapacity() / 20, weightRemoved = 0;
                        int removedCount = 0;
                        // loop starting with oldest so that the LRU ones are the
                        // first to be scaled down when applicable
                        for (Entry<Object[], Long> ent : scaleCopiesCandidates) {
                            try {
                                modelId = (String) ent.getKey()[0];
                                CacheEntry<?> ce = (CacheEntry<?>) ent.getKey()[2];
                                int weight = ce.getWeight();
                                boolean removed = removeModelCopies(modelId, (ModelRecord) ent.getKey()[1], ce,
                                        ent.getValue(), now,
                                        // allow removing our copy if none have been scaled down yet
                                        // or we're in the removal weight allowance
                                        removedCount == 0 || weight <= maxWeightToRemove);
                                if (removed) {
                                    removedCount++;
                                    maxWeightToRemove -= weight;
                                    weightRemoved += weight;
                                }
                            } catch (Exception e) {
                                logger.error("Janitor exception while scaling down copies"
                                             + (modelId != null ? " for model " + modelId : ""), e);
                            }
                        }
                        if (removedCount > 0) {
                            logger.info("Janitor pruned " + removedCount + " model copies (~"
                                        + mb(weightRemoved * UNIT_SIZE) + ")");
                        }
                    }

                    // probably demote this to debug later, or do based on some time threshold
                    logger.info("Janitor registry pruning task took " + msSince(before) + "ms for " + j + "/" + i
                                + " entries");

                    // these are always logged in case there are no instances changes happening
                    if (!shuttingDown && metrics.isEnabled()) {
                        metrics.logInstanceStats(instanceInfo.get(instanceId));
                    }
                } finally {
                    curThread.setName(threadNameBefore);
                }
            }

            // cutoff time random between 6 and 7 hours, to avoid m-m instances containing the
            // same model attempting the update concurrently
            private final long minStaleAge = 6 * 3600_000L + (long) (3600_000 * Math.random());

            // ModelRecord instance should not be used after calling this meth (might be stale)
            private void updateLastUsedTimeInRegistryIfStale(String modelId, ModelRecord mr, long lastUsed)
                    throws Exception {
                if (lastUsed == Long.MAX_VALUE) {
                    // This can be the case for certain failure placeholder records
                    return;
                }
                // only attempt update of model record lastUsed time if it's more than
                // minStaleAge out of date.
                long recLastUsed = mr.getLastUsed();
                if (lastUsed - recLastUsed < minStaleAge) {
                    return;
                }
                mr.updateLastUsed(lastUsed);
                // don't care if this succeeds or not - it will be re-attempted next time around
                if (registry.conditionalSet(modelId, mr)) {
                    long now = currentTimeMillis();
                    logger.info("Updated stale lastUsed time in registry for model " + modelId + " from " + recLastUsed
                                + " (" + readableTime(now - recLastUsed) + " ago)" + " to " + lastUsed + " ("
                                + readableTime(now - lastUsed) + " ago)");
                }
            }
        };
    }

    /*
     * The logic in this method is invoked by the local janitor task, and "scales down"
     * "scales down" the number of loaded copies of a model (across multiple instances)
     * based on how recently it was used.
     *
     * The corresponding scale-up is handled by the rateTrackingTask.
     *
     */
    private boolean removeModelCopies(String modelId, ModelRecord mr, CacheEntry<?> ce, long lastUsed,
                                      long nowMillis, boolean canRemove) {
        if (lastUsed == 0L) {
            return false; // gone, nothing to do
        }

        int numInstances = mr.getInstanceIds().size();

        /* If there are also copies of the model in other instances, determine whether
         * the number should be reduced.
         *
         * If the count == 2 then the scale-down decision is based on how long ago the
         * model was used, relative to the overall state of the distributed cache.
         * Hysteresis here ensures there won't be flip-flopping. The decision as to
         * whether *we* should be the instance to flush the second model is based on
         * us being the *least* desirable location for new model placements.
         *
         * If the count > 2 then the model was most likely scaled due to high
         * req load and the scale-down decision is based on various factors,
         * primarily observed req load for the model in *this* instance, and time
         * since last load or unload of the model across the cluster.
         *
         * Taking the time of the prior most recent load or unload of the model
         * (in any instance) into account ensures that multiple instances won't
         * make the same scale-down decision for the same model at the same time.
         */
        if (!canRemove || numInstances < 2) {
            return false;
        }

        // only unload if cluster is close to full
        ClusterStats stats = instanceSetStats();
        if (stats.totalCapacity == 0 || stats.totalFree * 100L / stats.totalCapacity > 5) {
            return false;
        }

        // find any other "valid" instances which have this model loaded
        Entry<String, InstanceRecord> otherValidInstance = null;
        for (String iid : mr.getInstanceIds().keySet()) {
            if (!instanceId.equals(iid)) {
                InstanceRecord ir = instanceInfo.get(iid);
                if (ir != null && !ir.isShuttingDown()) {
                    otherValidInstance = Maps.immutableEntry(iid, ir);
                    break;
                }
            }
        }

        // don't scale down if there's no other copy in a valid instance
        if (otherValidInstance == null) {
            return false;
        }

        if (numInstances == 2) {
            // if loaded in 2 places, reduce if old enough
            long lastHeavyTime = ce.getLastHeavyTime(), cacheAge = nowMillis - stats.globalLru;
            long scaleDownAge = cacheAge / 10L; // 10% of cache "age"
            if (lastHeavyTime == 0 || (nowMillis - lastHeavyTime) < cacheAge / 5L) {
                // unless model has recent "heavy" usage, cap scale-down age at 16 hours
                scaleDownAge = Math.min(SECOND_COPY_REMOVE_MAX_AGE_MS, scaleDownAge);
            }
            if ((nowMillis - lastUsed) > scaleDownAge) {
                return removeSecondModelCopy(modelId, mr, ce, lastUsed, otherValidInstance);
            }
        } else { // numOtherInstances > 1, i.e. num total copies >= 3

            // don't unload if another copy was very recently unloaded or loaded
            long lastUnloadTime = mr.getLastUnloadTime();
            if (lastUnloadTime > 0 && nowMillis - lastUnloadTime < 8 * RATE_CHECK_INTERVAL_MS) {
                return false;
            }
            // don't unload if a copy was very recently loaded (within last 30mins)
            if (loadedSince(mr, nowMillis - 1_800_000L, null)) {
                return false;
            }

            // cutoff age used for checking last time a model's req load was above the
            // scale-up threshold is based on global LRU - ranges from 10 mins to 5 hrs
            long minAgeMs = (3L * stats.globalLru + 10_400_000L) / 100L;
            if (minAgeMs < 600_000L) minAgeMs = 600_000L; // 10min
            else if (minAgeMs > 18_000_000L) minAgeMs = 18_000_000L; // 5 hrs
            // don't unload if the most recent heavy load was less than minAgeMs ago
            if (nowMillis - ce.getLastHeavyTime() < minAgeMs) {
                return false;
            }

            //TODO maybe make the above age vary based on how many >2 model copies
            // there are relative to # models loaded
            //    -- but how best to efficiently keep track of that num

            long timeSinceLastCheck = nowMillis - lastCheckTime;
            // sample period too small
            if (timeSinceLastCheck < (RATE_CHECK_INTERVAL_MS / 10)) {
                return false;
            }
            long rpm = ce.getRpm(timeSinceLastCheck);

            MaxConcCacheEntry<?> mcce = ce instanceof MaxConcCacheEntry ? (MaxConcCacheEntry<?>) ce : null;
            long threshold = mcce == null ? scaleUpRpmThreshold : mcce.getRpmScaleThreshold(false);

            // don't unload if the *current* load is > 2/3 of the scale-up threshold
            if (rpm > (threshold * 2) / 3) {
                return false;
            }

            // don't unload if there is more than one queued request
            if (mcce != null && mcce.queuedRequestCount() > 1) {
                return false;
            }

            return removeLocalModelCopyAsync(modelId, mr, ce, lastUsed);
        }
        return false;
    }

    /* Used for scaling down from 2 to 1 copies
     */
    protected boolean removeSecondModelCopy(String modelId, ModelRecord mr, CacheEntry<?> ce, final long lastUsed,
            Entry<String, InstanceRecord> otherValidInstance) {
        // ok, decision is made to remove a copy - determine if it should be ours

        if (otherValidInstance == null) {
            return false; // should not be the case
        }
        InstanceRecord thisIr = instanceInfo.get(instanceId);
        if (thisIr == null || thisIr.isShuttingDown()) {
            return false;
        }
        Entry<String, InstanceRecord> thisInst = Maps.immutableEntry(instanceId, thisIr);
        // ensure we are the least desirable place for new models relative to the
        // other instance which contains this model
        if (PLACEMENT_ORDER.compare(otherValidInstance, thisInst) > 0) {
            // the other instance is a better candidate to flush the model
            // from, so we abort. That instance's janitor thread should
            // be the one to do the flushing
            return false;
        }
        return removeLocalModelCopyAsync(modelId, mr, ce, lastUsed);
    }

    /**
     * Used for scaling down from > 2 copies (or from 2 to 1 if via removeSecondModelCopy method)
     *
     * @param modelId
     * @param mr       should *not* be used after invoking this method (passed to async task)
     * @param ce
     * @param lastUsed
     */
    protected boolean removeLocalModelCopyAsync(final String modelId, final ModelRecord mr, final CacheEntry<?> ce,
            final long lastUsed) {
        Long regLoadTime = mr.getInstanceIds().get(instanceId);
        if (regLoadTime == null || regLoadTime != ce.loadTimestamp) {
            return false;
        }

        // dispatch to another thread to do the final check and remove
        taskPool.execute(() -> {
            try {
                // do a final verification that another loaded copy of the
                // model can be "reached" before we eject ours
                boolean loadedElsewhere = isLoadedElsewhere(modelId);
                if (!loadedElsewhere) {
                    return;
                }

                // evict model and remove from registry
                mr.getInstanceIds().remove(instanceId);
                mr.updateLastUnloadTime();
                mr.updateLastUsed(lastUsed);
                ce.lastUnloadAttemptTime = currentTimeMillis();
                if (registry.conditionalSet(modelId, mr)) {
                    logger.info("Removing copy of model " + modelId + " from instance " + instanceId
                                + " due to scale-down. Model now has " + mr.getInstanceIds().size() + " loaded copies");
                    ce.remove();
                }
                // else no big deal, we'll try again next time around
            } catch (Exception e) {
                logger.warn("Exception checking/flushing copy of model " + modelId
                            + " from instance " + instanceId, e);
            }
        });
        return true;
    }

    protected void ensureLoadedInternalAsync(String modelId, long lastUsed, int weight, List<String> toExclude,
            int chainedLoadCount) {
        // dispatch to another thread to actually invoke ensureLoaded
        taskPool.execute(() -> {
            try {
                ensureLoadedInternal(modelId, lastUsed, weight, toExclude, chainedLoadCount, false);
            } catch (Exception e) {
                logger.warn("Error triggering/ensuring additional load of model " + modelId, e);
            }
        });
    }

    static final class ModelToLoad implements Comparable<ModelToLoad> {
        final String modelId;
        final long lastUsed;
        final int index;

        ModelToLoad(String modelId, long lastUsed, int index) {
            this.modelId = modelId;
            this.lastUsed = lastUsed;
            this.index = index;
        }

        @Override
        public int compareTo(ModelToLoad m) {
            // Note that this is a reverse ordering, for descending sort by lastUsed time
            return Long.compare(m.lastUsed, lastUsed);
        }
    }

    /*
     * Reaper task scours kv model registry for orphaned registrations, and also proactively triggers
     * loading of currently unloaded models if there is sufficient free space in the cluster.
     *
     * Runs only in the leader instance
     */
    private ScheduledFuture<?> reaperTask;

    //TODO as well as periodic, trigger reaper task when new instances are added
    private synchronized void leaderChange(boolean leader) {
        if (leader) {
            logger.info("This instance elected leader");
            // ship metrics when picking up leadership
            logModelCountMetrics();
            metrics.registerGlobals();
            if (reaperTask == null) {
                missings.clear(); // precautionary

                // process vmodels (ensure any in-progress transitions have a watching thread)
                if (vModelManager != null) {
                    vModelManager.processVModels();
                }

                reaperTask = taskPool.scheduleWithFixedDelay(new Runnable() {
                    @Override
                    public void run() {
                        if (shuttingDown) {
                            return;
                        }
                        Thread curThread = Thread.currentThread();
                        String threadNameBefore = curThread.getName();
                        curThread.setName("reaper-task");
                        try {
                            LeaderElection ll = leaderLatch;
                            if (ll != null && !ll.isLeader()) {
                                return;
                            }

                            if (!verifyKvStoreConnection()) {
                                logger.warn("Skipping run of reaper task due to failed kv-store conn check");
                                return;
                            }

                            long beforeNanos = nanoTime();

                            ClusterStats globalStats = clusterStats;
                            long globalLru = 0L;
                            List<Entry<String, ModelRecord>> proactiveLoadCandidates = null;
                            if (!DISABLE_PROACTIVE_LOADING && globalStats.totalCapacity > 0) {
                                proactiveLoadCandidates = new ArrayList<>();
                                // here, globalLru == 0L <=> cluster has free space
                                globalLru = globalStats.totalFree > 0 ? 0L : globalStats.globalLru;
                            }

                            // Prune any missing instances from the model registry, and collect candidates
                            // for subsequent proactive model loads
                            boolean kvConnectionOk = pruneModelRegistry(proactiveLoadCandidates, globalLru);

                            // Trigger proactive loading of unloaded models if there is free space in the cluster
                            // or unloaded models were used more recently than some currently loaded
                            if (proactiveLoadCandidates != null && !proactiveLoadCandidates.isEmpty()) {
                                try {
                                    if (typeConstraints == null) {
                                        triggerProactiveLoadsForInstanceSubset(clusterStats, proactiveLoadCandidates, null);
                                    } else {
                                        boolean first = true;
                                        for (InstanceSetStatsTracker isst : typeConstraints.getPartitionStats()) {
                                            if (first) {
                                                first = false;
                                            } else {
                                                // Wait to ensure that cluster stats reflect the last set of loads
                                                // (since types may span multiple subsets and so prior iterations
                                                // could also affect later instance subsets)
                                                Thread.sleep(INSTANCE_REC_PUBLISH_MIN_PERIOD_MS * 2L);
                                            }
                                            triggerProactiveLoadsForInstanceSubset(isst.currentStats,
                                                    proactiveLoadCandidates, isst.prohibitedTypesSet);
                                        }
                                    }
                                } catch (InterruptedException ie) {
                                    logger.warn("Reaper interrupted while processing proactive model loads, aborting");
                                    return;
                                }
                            }

                            // process vmodels (ensure any in-progress transitions have a watching thread)
                            if (vModelManager != null) {
                                vModelManager.processVModels();
                            }

                            // clean any "stuck" leaseless instance records (previously possible due to old bug)
                            if (kvConnectionOk) {
                                cleanLeaselessEtcdInstanceRecords();
                            }

                            // log model counts, in case they aren't changing much
                            logModelCountMetrics();

                            logger.info("Reaper task processing took " + msSince(beforeNanos) + "ms");
                        } finally {
                            curThread.setName(threadNameBefore);
                        }
                    }

                    /**
                     * This checks every model occurrence in the registry to make sure the
                     * corresponding instance still exists. if that instance has been missing
                     * for longer than the threshold then clean up the registration.
                     *
                     * @param proactiveLoadCandidates null if not required, otherwise to-be-filled
                     * @param globalLru 0L if the cluster is not full, global LRU otherwise
                     * @return false if kv store errors encountered
                     */
                    boolean pruneModelRegistry(
                            List<Entry<String, ModelRecord>> proactiveLoadCandidates, long globalLru) {
                        //TODO consider parallelization of this stage, have seen it take ~10min
                        // (handful of instances gone, each had 1000's model copies)

                        int kvConnectionErrorCount = 0;
                        String modelId = null;
                        int totalPrunedCount = 0;

                        // First perform pruning and collect full list of candidate models for
                        // proactive loading
                        Set<String> notFoundInstances = new HashSet<>();
                        for (Entry<String, ModelRecord> ent : registry.noThrowIterable()) {
                            try {
                                modelId = ent.getKey();
                                ModelRecord mr = ent.getValue();
                                while (true) { // this loop just for conditional update retries
                                    Map<String, Long> insts = mr.getInstanceIds(),
                                            failInsts = mr.getLoadFailedInstanceIds();
                                    if (readOnlyMode) {
                                        // don't perform any pruning of the model registry

                                        // for proactive load decisions, ignore instances
                                        // which aren't registered in the same kv store
                                        if (!insts.isEmpty()) {
                                            insts = Maps.filterKeys(insts, instanceInfo::contains);
                                        }
                                    } else {
                                        int modCount = pruneMissingInstances(insts, Collections.emptyMap(), notFoundInstances)
                                                + pruneMissingInstances(failInsts, mr.getLoadFailureInfos(), notFoundInstances);
                                        if (modCount > 0) {
                                            ModelRecord newMr = registry.conditionalSetAndGet(modelId, mr);
                                            if (newMr != mr) {
                                                mr = newMr;
                                                if (mr == null) {
                                                    break; // continue outer for loop
                                                }
                                                continue; // retry with updated model record
                                            }
                                            // model record pruned successfully
                                            totalPrunedCount += modCount;
                                        }
                                        // else no pruning was done
                                    }

                                    // Check for invalid lastUsed field value and repair if needed
                                    repairLastUsedTimeIfNeeded(modelId, mr);

                                    // If we have room and this model is not loaded, add it to the list
                                    // to be loaded proactively
                                    if (proactiveLoadCandidates != null && insts.isEmpty() && failInsts.size() < 2
                                        && (globalLru == 0L || mr.getLastUsed() > globalLru)) {
                                        proactiveLoadCandidates.add(ent);
                                    }
                                    break; // continue outer for loop
                                }
                            } catch (Exception e) { // end outer for loop
                                logger.warn("Exception while housekeeping registry"
                                            + (modelId != null ? " for model: " + modelId : ""), e);
                                if (io.grpc.Status.fromThrowable(e).getCode() == Code.UNAVAILABLE) {
                                    if (++kvConnectionErrorCount >= 3) {
                                        logger.info("Skipping remaining models on this reaper iteration");
                                        break;
                                    }
                                }
                            }
                        }

                        if (totalPrunedCount > 0) {
                            logger.info("registry-reaper pruned " + totalPrunedCount
                                        + " model instance registrations");
                            logger.info("Missing instance map for pruning: " + missings);
                        }
                        if (kvConnectionErrorCount >= 3) {
                            proactiveLoadCandidates.clear();
                            return false;
                        }
                        if (!missings.isEmpty()) {
                            // clean out old entries from missings map
                            long now = currentTimeMillis();
                            missings.entrySet().removeIf(ent -> (now - ent.getValue()) > ASSUME_INSTANCE_GONE_AFTER_MS
                                                                || instanceInfo.contains(ent.getKey()));
                            logger.info("Missing instance map after cleanup: " + missings);
                        }
                        return true;
                    }

                    /**
                     * @param stats
                     * @param allCandidates selected candidates will be nulled from list
                     * @param excludeTypes model types to exclude, must be sorted
                     */
                    void triggerProactiveLoadsForInstanceSubset(ClusterStats stats,
                            List<Entry<String, ModelRecord>> allCandidates,
                            ProhibitedTypeSet excludeTypes) throws InterruptedException {
                        // get free units
                        int freeSpaceProactiveLoadCount = 0, totalProactiveLoadCount = 0;
                        if (stats.totalCapacity > 0 && stats.totalFree > 0) {
                            int sizeEstimate;
                            if (stats.modelCopyCount < 3) {
                                sizeEstimate = defaultModelSizeUnits;
                            } else {
                                int averageSize = (int) (stats.totalCapacity - stats.totalFree) / stats.modelCopyCount;
                                sizeEstimate = stats.modelCopyCount > 10 ? averageSize
                                        : (averageSize + defaultModelSizeUnits) / 2;
                            }

                            // each time aim to fill ~ half of the free space on each
                            // instance which is *above* ~12.5% of its total capacity
                            long spaceToFill = 0L;
                            for (Entry<String, InstanceRecord> ent : clusterState) {
                                InstanceRecord ir = ent.getValue();
                                if (excludeTypes != null && !excludeTypes.equals(ir.prohibitedTypes)) {
                                    continue;
                                }
                                // skip instances with already significant loading queue
                                int maxLoads = ir.getLoadingThreads() * 50 - ir.getLoadingInProgress();
                                if (maxLoads <= 0) {
                                    continue;
                                }
                                long reserve = ir.getCapacity() / 8, avail = ir.getRemaining() - reserve;
                                if (avail > 0L) {
                                    spaceToFill += Math.min(avail, maxLoads * sizeEstimate);
                                }
                            }
                            spaceToFill /= 2;

                            freeSpaceProactiveLoadCount = (int) (spaceToFill / sizeEstimate);
                            // allow up to 5% of the cluster capacity to be replaced each run
                            // (Math.max is intended below - ok for the number to be bigger if
                            // there's a lot of free space)
                            totalProactiveLoadCount = Math.max(freeSpaceProactiveLoadCount,
                                    (int) (stats.totalCapacity / (20L * sizeEstimate)));
                        }

                        // when cache is full, only proactively load non-loaded models which have a
                        // last-used time more recent than at least ~33% of the loaded models (since
                        // they will be replacing older loaded models)
                        long proactiveLastUsedCutoff = stats.globalLru == Long.MAX_VALUE ? 0L
                                // minimum of 20 minutes here to avoid churn in pathological cases
                                : stats.globalLru + Math.max(age(stats.globalLru) / 3L, 1_200_000L);
                        if (logger.isDebugEnabled()) {
                            logger.debug(excludeTypes == null ? "" :
                                    ("PTS subset " + excludeTypes + " (" + stats.instanceCount + " instances):")
                                    + " maxProactiveLoadCount=" + totalProactiveLoadCount
                                    + " >= freeSpaceProactiveLoadCount=" + freeSpaceProactiveLoadCount
                                    + "; proactiveLastUsedCutoff=" + (proactiveLastUsedCutoff == 0L ? "none"
                                            : readableTime(age(proactiveLastUsedCutoff))
                                              + " totalCandidates=" + allCandidates.size()));
                        }

                        NavigableSet<ModelToLoad> toLoad = null; // lazy-instantiated
                        for (int i = 0, l = allCandidates.size(); i < l; i++) {
                            Entry<String, ModelRecord> ent = allCandidates.get(i);
                            if (ent == null) {
                                continue;
                            }
                            ModelRecord mr = ent.getValue();
                            if (excludeTypes != null && excludeTypes.contains(mr.getType())) {
                                continue;
                            }
                            long lastUsed = mr.getLastUsed();
                            if (totalProactiveLoadCount > 0
                                && (freeSpaceProactiveLoadCount > 0
                                    || lastUsed > proactiveLastUsedCutoff)) {
                                if (toLoad == null) toLoad = new TreeSet<>();
                                if (toLoad.size() < totalProactiveLoadCount
                                        || toLoad.last().lastUsed < lastUsed) {
                                    toLoad.add(new ModelToLoad(ent.getKey(), lastUsed, i));
                                    if (toLoad.size() > totalProactiveLoadCount) {
                                        toLoad.pollLast();
                                    }
                                }
                            }
                        }
                        if (toLoad == null || toLoad.isEmpty()) {
                            return;
                        }

                        // process list of models to be proactively loaded
                        logger.info("About to trigger proactive loading of " + toLoad.size() +
                                    " currently unloaded models" + (excludeTypes == null ? ""
                                : " for instance subset with " + excludeTypes + " and " + stats));
                        long beforeNanos = nanoTime();
                        int count = 0;
                        final Phaser phaser = new Phaser(1);
                        // loop in order of most recently used to least recently modified used
                        for (ModelToLoad modelToLoad : toLoad) {
                            final long timestamp = modelToLoad.lastUsed;
                            // non-loaded models of all ages can fill up free space, but after
                            // that only include those which are newer than the cutoff (since
                            // they will cause already loaded models to be evicted)
                            if (freeSpaceProactiveLoadCount > 0) {
                                freeSpaceProactiveLoadCount--;
                            } else if (timestamp < proactiveLastUsedCutoff) {
                                break;
                            }

                            final String fModelId = modelToLoad.modelId;
                            phaser.register();
                            allCandidates.set(modelToLoad.index, null); // null out loaded model
                            taskPool.execute(() -> {
                                try {
                                    ensureLoadedInternal(fModelId, timestamp, 0, null, 0, false);
                                } catch (Exception e) {
                                    logger.warn("Exception from proactive load of model " + fModelId, e);
                                } finally {
                                    phaser.arrive();
                                }
                            });
                            count++;
                        }
                        try {
                            // This does not wait for all the loadings to complete since sync=false
                            // is passed to ensureLoaded, but should wait until all the loads have
                            // been initiated
                            phaser.awaitAdvanceInterruptibly(phaser.arrive());
                            logger.info("Took " + msSince(beforeNanos) + "ms to complete load initiation"
                                        + " attempt(s) for " + count + " models");
                        } catch (InterruptedException ie) {
                            logger.warn("Interrupted while waiting for initiation of proactive model loads");
                            throw ie;
                        }
                    }

                    /**
                     * @return number of missing instances which were removed from the map
                     */
                    private int pruneMissingInstances(Map<String, Long> instances, Map<String, ?> secondary,
                            Set<String> notFound) throws Exception {
                        if (instances.isEmpty()) {
                            return 0;
                        }
                        int modCount = 0;
                        long now = currentTimeMillis();
                        for (Iterator<Entry<String, Long>> it = instances.entrySet().iterator(); it.hasNext(); ) {
                            final Entry<String, Long> ent = it.next();
                            if (now - ent.getValue() < ASSUME_INSTANCE_GONE_AFTER_MS) {
                                continue; // ignore recently loaded
                            }
                            final String instId = ent.getKey();
                            if (instanceId.equals(instId)) {
                                continue; // we think therefore we are
                            }
                            if (!notFound.contains(instId)) {
                                boolean present = instanceInfo.contains(instId);
                                if (present || instanceInfo.getStrong(instId) != null) {
                                    continue;
                                }
                                notFound.add(instId);
                            }
                            // the instance is missing!
                            final Long missingSince = missings.putIfAbsent(instId, now);
                            if (missingSince != null && (now - missingSince) > ASSUME_INSTANCE_GONE_AFTER_MS) {
                                it.remove();
                                secondary.remove(instId);
                                modCount++;
                            }
                        }
                        return modCount;
                    }

                    // clean any "stuck" leaseless instance records (possible due to prior bug)
                    private void cleanLeaselessEtcdInstanceRecords() {
                        if (!"etcd".equals(kvUtilsFactory.getKvStoreType())) {
                            return;
                        }
                        Set<String> registeredInstances = null;
                        for (Entry<String, InstanceRecord> ent : instanceInfo) {
                            InstanceRecord ir = ent.getValue();
                            if (ir.getLeaseId() == 0) {
                                if (registeredInstances == null) {
                                    List<ServiceInstanceInfo> siis = ((LitelinksServiceClient) runtimeClient)
                                            .getServiceInstanceInfo();
                                    registeredInstances = new HashSet<>(siis.size());
                                    for (ServiceInstanceInfo sii : siis) {
                                        registeredInstances.add(sii.getInstanceId());
                                    }
                                }
                                String iid = ent.getKey();
                                // only delete if instance doesn't have a litelinks registration
                                if (!registeredInstances.contains(iid)) {
                                    try {
                                        if (instanceInfo.conditionalDelete(iid, ir)) {
                                            logger.info("Deleted stale leaseless instance table record with id: " + iid);
                                        }
                                        // else conflict, no problem will catch next time if still needed
                                    } catch (Exception e) {
                                        logger.warn("Error trying to delete stale instance record " + iid, e);
                                    }
                                }
                            }
                        }
                    }
                }, readOnlyMode ? 1L : 2L, REGISTRY_REAPER_FREQ_MINS, MINUTES);
            }
        } else {
            metrics.unregisterGlobals();
            ScheduledFuture<?> pt = reaperTask;
            if (pt != null) {
                pt.cancel(false);
                reaperTask = null;
            }
            missings.clear();
            logger.info("This instance lost leadership");
            try {
                logger.info("Leader is now: " + leaderLatch.getLeaderId());
            } catch (Exception e) {
                logger.warn("New leader could not be determined", e);
            }
        }
    }

    final void repairLastUsedTimeIfNeeded(String modelId, ModelRecord mr) throws Exception {
        // Some ModelRecords records were observed in the registry with an invalid lastUsed
        // time of Long.MAX_VALUE, effectively pinning them at the front of the cache and
        // causing them to remain loaded even if not used.
        // It's not clear whether the bug causing this has yet been fixed - for now we repair
        // the entries and can monitor logs for the warning message below.
        if (mr != null && mr.getLastUsed() == Long.MAX_VALUE) {
            mr.setLastUsed(currentTimeMillis() - (LASTUSED_AGE_ON_ADD_MS * 3L));
            if (registry.conditionalSet(modelId, mr)) {
                logger.warn("Repaired Long.MAX_VALUE lastUsed time in registry for model " + modelId);
            }
            // Don't worry if this fails, will try again next time around
        }
    }

    private void logModelCountMetrics() {
        if (!metrics.isEnabled()) {
            return;
        }
        int lmc = loadedModelCount;
        if (lmc < 0) {
            return;
        }
        metrics.logGaugeMetric(Metric.MODELS_LOADED, lmc);
        metrics.logGaugeMetric(Metric.MODELS_WITH_LOAD_FAIL, failedModelCount);
        metrics.logGaugeMetric(Metric.TOTAL_MODELS, registry.getCount());
    }

    protected boolean verifyKvStoreConnection() {
        try {
            return kvUtilsFactory.verifyKvStoreConnection().get(4, SECONDS);
        } catch (Exception e) {
            Throwable t = e instanceof ExecutionException ? e.getCause() : e;
            logger.error("KV Store connection verification failed: " + t); // don't log whole stacktrace
            return false;
        }
    }

    private static final Comparator<Entry<?, Long>> VALUE_COMP = (e1, e2) -> {
        Long l1 = e1.getValue(), l2 = e2.getValue();
        if (l1 == null || l2 == null) {
            return l1 == l2 ? 0 : (l1 == null ? -1 : 1);
        }
        return l1.compareTo(l2);
    };

    protected List<String> excludeThisInstance; // set in initialize() method

    /**
     * @return true if the service is loaded elsewhere,
     * false if it isn't, *or* might be but can't be verified
     */
    protected boolean isLoadedElsewhere(String modelId) {
        try {
            ThreadContext.removeCurrentContext();
            StatusInfo si = internalOperation(modelId, true, false, false, -1L, excludeThisInstance);
            return si != null && si.getStatus() == Status.LOADED;
        } catch (Exception e) {
            //TODO maybe log here
            return false;
        }
    }

    /*
     * Used by out-of-band tasks to verify this model is loaded in another instance
     * and if not, asynchronously trigger loading of it in some other instance,
     * regardless of whether it's already loaded in this instance.
     */
    protected StatusInfo ensureLoadedElsewhere(String modelId, long lastUsedTime, int weight) throws InternalException {
        return ensureLoadedInternal(modelId, lastUsedTime, weight, excludeThisInstance, 0, false);
    }

    /*
     * Used by out-of-band tasks to asynchronously trigger loading of this model in
     * some instance where it's not already loaded/loading, excluding this one.
     */
    protected StatusInfo triggerNewModelCopyElsewhere(String modelId, ModelRecord mr, long lastUsedTime, int weight)
            throws InternalException {
        Set<String> current = mr.getInstanceIds().keySet();
        boolean includesUs = current.contains(instanceId);
        List<String> toExclude;
        if (current.isEmpty() || current.size() == 1 && includesUs) {
            toExclude = excludeThisInstance;
        } else {
            toExclude = new ArrayList<>(current.size() + 1);
            toExclude.addAll(current);
            if (!includesUs) {
                toExclude.add(instanceId);
            }
        }
        return ensureLoadedInternal(modelId, lastUsedTime, weight, toExclude, 0, false);
    }

    protected StatusInfo ensureLoadedInternal(String modelId, long lastUsedTime, int weight, List<String> toExclude,
            int chainedLoadCount, boolean sync) throws InternalException {
        ThreadContext.removeCurrentContext(); // ensure context is clear
        // inform other instances of the weight for more accurate initial sizing
        if (weight > INSERTION_WEIGHT) {
            ThreadContext.addContextEntry(KNOWN_SIZE_CXT_KEY, String.valueOf(weight));
        }
        if (chainedLoadCount > 0) {
            ThreadContext.addContextEntry(CHAINED_LOAD_COUNT_KEY, String.valueOf(chainedLoadCount));
        }
        if (toExclude == null || !toExclude.contains(instanceId)) {
            // add flag to ensure we aren't specially favoured
            ThreadContext.addContextEntry(UNBALANCED_KEY, "true");
        }
        ThreadContext.addContextEntry(TAS_INTERNAL_CXT_KEY, INTERNAL_REQ);
        // Set 3 second timeout or 10 minute "safeguard" if blocking
        ThreadContext.setDeadlineAfter(sync ? 600L : 3L + chainedLoadCount, SECONDS);
        try {
            return ensureLoaded(modelId, lastUsedTime, toExclude, sync, true);
        } catch (ModelNotFoundException mnfe) {
            return SI_NOT_FOUND;
        }
    }

    /* --------------------------------- shutdown/migration logic -------------------------------------------- */

    protected volatile boolean shuttingDown;

    @Override
    protected void preShutdown() {
        shuttingDown = true;

        long timeAllowed = Math.max(2, DefaultThriftServer.SVC_IMPL_SHUTDOWN_TIMEOUT_SECS - 5);
        long shutdownDeadlineNanos = nanoTime() + NANOSECONDS.convert(timeAllowed, SECONDS);

        logger.info("Starting pre-shutdown with deadline in " + timeAllowed
                    + "secs; removing ourselves from instance table");

        // remove from clusterState
        boolean foundOther = false;
        for (Iterator<Entry<String, InstanceRecord>> it = clusterState.iterator(); it.hasNext(); ) {
            if (instanceId.equals(it.next().getKey())) {
                it.remove();
            } else {
                foundOther = true;
            }
        }

        // this sets the shutting-down flag in our instance record,
        // which will prevent new load requests coming our way
        try {
            publishInstanceRecord(true, true);
        } catch (Exception e) {
            logger.warn("Exception updating instance record pre-shutdown", e);
        }

        abortLoadings();

        boolean debug = logger.isDebugEnabled();
        // the returned map is a private copy
        Map<String, Long> cacheEntries = runtimeCache.descendingLruMap();
        if (unloadManager != null) {
            unloadManager.removeUnloadBufferEntry(cacheEntries);
        }
        if (foundOther) {
            if (!cacheEntries.isEmpty()) {
                logger.info("Pre-shutdown - starting distribution of " + cacheEntries.size()
                            + " models to other instances");
                List<Future<Entry<String, Long>>> waitFor = new ArrayList<>(cacheEntries.size());
                final List<String> excludeThis = Collections.singletonList(instanceId);
                final long cutoff = currentTimeMillis() - CUTOFF_AGE_MS;
                final Set<String> deregistered = ConcurrentHashMap.newKeySet();

                // loop over local cache most recently used to least recent
                int willBeSkipped = 0;
                for (Entry<String, Long> ent : cacheEntries.entrySet()) {
                    String modelId = ent.getKey();
                    final ModelRecord mr = registry.get(modelId);
                    if (mr == null || !mr.getInstanceIds().containsKey(instanceId)) {
                        continue;
                    }
                    long lruT = ent.getValue();
                    if (lruT < cutoff) {
                        willBeSkipped++;
                    }
                    waitFor.add(taskPool.submit(() -> {
                        try {
                            CacheEntry<?> ce = runtimeCache.getQuietly(modelId);
                            if (ce == null || ce.isFailed()) {
                                return null; // already gone or failed
                            }
                            long lruTime = lruT != 0L ? lruT : runtimeCache.getLastUsedTime(modelId);
                            if (lruTime >= 0) {
                                ce.remove();
                            }
                            // deregister now if it's "safe" i.e. cache entry future was aborted
                            // and so can't yet be servicing any requests
                            if (ce.isAborted()) {
                                deregisterModelAsync(modelId, lruTime, ce.loadTimestamp, ce.loadCompleteTimestamp);
                                deregistered.add(modelId);
                            }
                            if (lruTime > 0L) {
                                StatusInfo status = triggerNewModelCopyElsewhere(modelId, mr, lruTime, ce.getWeight());
                                if (status.getStatus() == Status.LOADING_FAILED) {
                                    logger.warn("Failed to ensure model loaded elsewhere: " + modelId
                                            + (status.getErrorMessages() != null
                                            ? (", " + status.getErrorMessages()) : ""));
                                } else if (status.getStatus() == Status.LOADING && lruTime >= cutoff) {
                                    return ent;
                                }
                            }
                        } catch (TException e) {
                            logger.warn("Error trying to ensure model is loaded elsewhere: " + ent.getKey(), e);
                        }
                        return null; // don't wait for this one
                    }));
                }

                if (!waitFor.isEmpty()) {
                    logger.info("Pre-shutdown - waiting for up to " + (waitFor.size() - willBeSkipped)
                                + " models to load elsewhere");
                    long before = nanoTime();
                    int waitedCount = 0;
                    boolean timeout = false;
                    for (Future<Entry<String, Long>> fut : waitFor) {
                        Entry<String, Long> ent;
                        long remaining = shutdownDeadlineNanos - nanoTime();
                        if (remaining <= 0L) {
                            timeout = true;
                            break;
                        }
                        try {
                            ent = fut.get(remaining, NANOSECONDS);
                            if (ent == null) {
                                continue; // (this means don't wait for this one)
                            }
                            waitedCount++;
                            String modelId = ent.getKey();
                            long lastUsed = ent.getValue();
                            ListenableFuture<?> waiter = taskPool.submit(() -> {
                                try {
                                    //TODO TBD poll or blocking approach
                                    //TODO this currently won't wait for *additional* copies to finish loading if there
                                    // already other fully-loaded copies. We might want to tweak this to do so for models
                                    // which have been used within the last small window of time (e.g. ~3x expected loading time),
                                    // to ensure the remaining copies aren't temporarily over-burdened
                                    StatusInfo status = ensureLoadedInternal(modelId, lastUsed, 0, excludeThis, 0, true);
                                    if (status.getStatus() == Status.LOADING_FAILED) {
                                        logger.warn("Model failed to load elsewhere: " + modelId
                                                + (status.getErrorMessages() != null ? (", "
                                                + status.getErrorMessages()) : ""));
                                    }
                                } catch (TException e) {
                                    logger.warn("Error waiting for model to be loaded elsewhere: " + modelId, e);
                                }
                            });
                            remaining = shutdownDeadlineNanos - nanoTime();
                            // wait for ensureLoaded to return
                            try {
                                if (remaining <= 0L) {
                                    throw new TimeoutException();
                                }
                                waiter.get(remaining, NANOSECONDS);
                            } catch (TimeoutException te) {
                                waiter.cancel(true);
                                throw te;
                            }

                            if (debug) {
                                logger.debug("Removing our entry for " + modelId + " from registry");
                            }
                            deregisterModelAsync(modelId, lastUsed, null, 0L);
                            deregistered.add(modelId);

                        } catch (InterruptedException e) {
                            logger.warn("Pre-shutdown interrupted");
                            Thread.currentThread().interrupt();
                            return; // abort pre-shutdown logic
                        } catch (ExecutionException e) {
                            logger.warn("Uncaught propagation exception", e.getCause());
                            continue;
                        } catch (TimeoutException te) {
                            timeout = true;
                            break;
                        }
                    }
                    if (timeout) {
                        logger.warn("Shutdown timeout expired before model propagation completed");
                    } else {
                        logger.info("Pre-shutdown - model propagation complete (waited for " + waitedCount + " out of "
                                    + waitFor.size() + "), took " + msSince(before) + "ms");
                    }
                }
                // deregister any remaining ones
                for (Entry<String, Long> ent : cacheEntries.entrySet()) {
                    String modelId = ent.getKey();
                    if (deregistered.contains(modelId)) continue;
                    if (debug) logger.debug("Removing our entry for " + modelId + " from registry");
                    deregisterModelAsync(modelId, ent.getValue(), null, 0L);
                }
            } else {
                logger.info("Local cache is empty, no models to propagate pre-shutdown");
            }
        } else {
            logger.info("Pre-shutdown - we are the only instance; won't attempt to propagate elsewhere");
            if (!cacheEntries.isEmpty()) {
                logger.info("Removing our " + cacheEntries.size() + " model entries from registry");
                for (Entry<String, Long> ent : cacheEntries.entrySet()) {
                    if (debug) {
                        logger.debug("Removing our entry for " + ent.getKey() + " from registry");
                    }
                    deregisterModelAsync(ent.getKey(), ent.getValue(), null, 0L);
                }
            }
        }

        logger.info("Pre-shutdown complete");
    }

    @Override
    protected void shutdown() {
        // first quiesce + stop external grpc server
        if (grpcServer != null) {
            try {
                //TODO consider adjusting this time based on
                //  elapsed since start of preShutdown
                grpcServer.shutdown(15, SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        abortLoadings();
        closeQuietly(leaderLatch, config);

        try {
            loadingPool.awaitTermination(3, SECONDS);
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for loadingpool termination", e);
            Thread.currentThread().interrupt();
        }

        taskPool.shutdown();
        closeQuietly(runtimeClient, cacheMissClient, directClient);

        try {
            // might be processing model deregister tasks
            taskPool.awaitTermination(10, SECONDS);
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for taskpool termination", e);
            Thread.currentThread().interrupt();
        }

        // log empty instance metrics here since we're about to disappear
        metrics.logInstanceStats(new InstanceRecord());

        closeQuietly(instanceTable, myNode);
        closeQuietly(vModelManager);
        closeQuietly(kvTable);
        closeQuietly(metrics);
        closeQuietly(preStopNettyServer);
        metrics = Metrics.NO_OP_METRICS;
    }

    protected void abortLoadings() {
        List<Runnable> aborted = loadingPool.shutdownNow();
        int numAborted = aborted.size();
        if (numAborted > 0) {
            logger.info("Aborting " + numAborted + " queued model loads");
            for (Runnable ce : aborted) {
                ((CacheEntry<?>) ce).remove();
            }
        }
    }

    public static void closeQuietly(Object... toclose) {
        if (toclose != null) for (Object o : toclose) if (o instanceof AutoCloseable) try {
            ((AutoCloseable) o).close();
        } catch (Exception e) {
            logger.warn("Exception closing " + o.getClass(), e);
        }
    }

    @VisibleForTesting
    public long getLoadTimeoutMs() {
        return loadTimeoutMs;
    }

    /* ---------------------------------------- helpers ------------------------------------------------------ */

    static String mb(long bytes) {
        return mb(bytes, "MiB");
    }

    static String mb(long bytes, String unit) {
        return (bytes + (Mi / 2)) / Mi + (unit == null ? "" : unit);
    }

    static String gb(long bytes) {
        return String.format("%.3fGiB", (float) bytes / (float) Gi);
    }

    static Exception getInterruptionCause(Throwable t) {
        while (t != null) {
            if (t instanceof InterruptedException || t instanceof InterruptedIOException
                    || t instanceof ClosedByInterruptException) {
                return (Exception) t;
            }
            t = t.getCause();
        }
        return null;
    }

    static boolean isInterruption(Throwable t) {
        return getInterruptionCause(t) != null;
    }

    static boolean isTimeout(Throwable t) {
        while (t != null) {
            if (t instanceof TimeoutException || (t instanceof TTransportException
                    && ((TTransportException) t).getType() == TTransportException.TIMED_OUT)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    protected static InternalException newInternalException(String message, Throwable cause) {
        InternalException ie = new InternalException();
        if (message != null) ie.setMessage(message);
        if (cause != null) ie.initCause(cause);
        ie.setCauseStacktrace(Throwables.getStackTraceAsString(ie));
        logger.error(message != null ? message : "Unexpected exception", cause);
        return ie;
    }

    protected static InternalException newInternalInterruptedException(Exception cause, String task) {
        InternalException ie = new InternalException();
        ie.setMessage("Request interrupted due to " + GrpcSupport.cancellationReason(Context.current())
                + " while waiting for " + task);
        if (cause != null) ie.initCause(cause);
        ie.setCauseStacktrace(Throwables.getStackTraceAsString(ie));
        logger.warn(ie.getMessage(), cause);
        return ie;
    }

    protected ModelLoadException newModelLoadException(String message, long timeout, Throwable cause) {
        ModelLoadException mle = new ModelLoadException();
        if (message != null) mle.setMessage(message);
        if (timeout != 0L) mle.setTimeout(timeout);
        mle.setInstanceId(instanceId);
        if (cause != null) {
            if (message == null) mle.setMessage(cause.getClass().getName() + ": " + cause.getMessage());
            mle.initCause(cause);
        }
        mle.setCauseStacktrace(Throwables.getStackTraceAsString(mle));
        return mle;
    }

    protected static String setThreadName(Thread curThread, String name) {
        String threadNameBefore = curThread.getName();
        curThread.setName(name);
        return threadNameBefore;
    }

    /**
     * For non-overloaded method names
     */
    protected static Method getMethod(Class<?> cls, String name) throws RuntimeException {
        for (Method meth : cls.getMethods()) {
            if (meth.getName().equals(name)) return meth;
        }
        throw new RuntimeException(new NoSuchMethodException(name));
    }

    public static long msSince(long nanoTime) {
        return (nanoTime() - nanoTime) / M;
    }

    public static String readableTime(long ms) {
        if (ms < 1000L) return "less than a second";
        if (ms < 15_000L) return "a few seconds";
        if (ms < 60_000L) return "less than a minute";
        if (ms < 90_000L) return "a minute";
        if (ms < 7_200_000L) return ((ms + 30_000L) / 60_000L) + " minutes";
        if (ms < 259_200_000L) return ((ms + 1_800_000L) / 3_600_000L) + " hours";
        if (ms < 1_814_400_000L) return ((ms + 43_200_000L) / 86_400_000L) + " days";
        return ((ms + 302_400_000L) / 604_800_000L) + " weeks";
    }

    protected String getRequestMethodName(Method method, Object[] args) {
        return method.getName();
    }
}
