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

import com.ibm.watson.prometheus.Counter;
import com.ibm.watson.prometheus.Gauge;
import com.ibm.watson.prometheus.Histogram;
import com.ibm.watson.prometheus.NettyMemoryExports;
import com.ibm.watson.prometheus.NettyServer;
import com.ibm.watson.prometheus.SimpleCollector;
import com.ibm.watson.prometheus.hotspot.BufferPoolsExports;
import com.ibm.watson.prometheus.hotspot.GarbageCollectorExports;
import com.ibm.watson.prometheus.hotspot.MemoryAllocationExports;
import com.ibm.watson.prometheus.hotspot.MemoryPoolsExports;
import com.ibm.watson.statsd.NonBlockingStatsDClient;
import com.ibm.watson.statsd.StatsDSender;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.StatsDClientErrorHandler;
import io.grpc.Status.Code;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Array;
import java.net.SocketAddress;
import java.nio.channels.DatagramChannel;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.ibm.watson.modelmesh.Metric.*;
import static com.ibm.watson.modelmesh.Metric.MetricType.*;
import static com.ibm.watson.modelmesh.ModelMesh.M;
import static com.ibm.watson.modelmesh.ModelMeshEnvVars.MMESH_CUSTOM_ENV_VAR;
import static com.ibm.watson.modelmesh.ModelMeshEnvVars.MMESH_METRICS_ENV_VAR;
import static java.util.concurrent.TimeUnit.*;

/**
 *
 */
interface Metrics extends AutoCloseable {
    boolean isPerModelMetricsEnabled();

    boolean isEnabled();
    void logTimingMetricSince(Metric metric, long prevTime, boolean isNano);

    void logTimingMetricDuration(Metric metric, long elapsed, boolean isNano, String modelId);

    void logSizeEventMetric(Metric metric, long value, String modelId);

    void logGaugeMetric(Metric metric, long value);

    void logCounterMetric(Metric metric);

    default void logInstanceStats(final InstanceRecord ir) {
        if (ir == null) {
            return;
        }

        // local instance stats
        long lru = ir.getLruTime();
        long localLru = lru > 0L ? lru : Long.MAX_VALUE;

        // sends instance metrics to logmet
        logGaugeMetric(INSTANCE_LRU_TIME, localLru / 1_000L); // timestamp in s
        logGaugeMetric(INSTANCE_LRU_AGE,
                localLru == Long.MAX_VALUE ? 0L : (System.currentTimeMillis() - localLru) / 1_000L); // age in s
        logGaugeMetric(INSTANCE_CAPACITY, ir.getCapacity());
        logGaugeMetric(INSTANCE_USED, ir.getUsed());
        // shipping usage range: 0 - 10_000
        logGaugeMetric(INSTANCE_USED_BASIS_POINTS,
                ir.getCapacity() == 0 ? 0 : (long) (1.0 * ir.getUsed() / ir.getCapacity() * 10_000));
        logGaugeMetric(INSTANCE_MODEL_COUNT, ir.getCount());
    }

    /**
     * Record per-request metrics
     *
     * @param external whether this is an internal or external request
     * @param name name of the request method (rpc)
     * @param elapsedNanos time taken in nanoseconds
     * @param code gRPC response code
     * @param reqPayloadSize request payload size in bytes (or -1 if not applicable)
     * @param respPayloadSize response payload size in bytes (or -1 if not applicable)
     */
    void logRequestMetrics(boolean external, String name, long elapsedNanos, Code code,
                           int reqPayloadSize, int respPayloadSize, String modelId, String vModelId);

    default void registerGlobals() {}

    default void unregisterGlobals() {}

    @Override
    default void close() {}

    Metrics NO_OP_METRICS = new Metrics() {
        @Override
        public boolean isPerModelMetricsEnabled() {
            return false;
        }

        @Override
        public boolean isEnabled() {
            return false;
        }

        @Override
        public void logTimingMetricSince(Metric metric, long prevTime, boolean isNano) {}

        @Override
        public void logTimingMetricDuration(Metric metric, long elapsed, boolean isNano, String modelId){}

        @Override
        public void logSizeEventMetric(Metric metric, long value, String modelId){}

        @Override
        public void logGaugeMetric(Metric metric, long value) {}

        @Override
        public void logCounterMetric(Metric metric) {}

        @Override
        public void logInstanceStats(InstanceRecord ir) {}

        @Override
        public void logRequestMetrics(boolean external, String name, long elapsedNanos, Code code,
                                      int reqPayloadSize, int respPayloadSize, String modelId, String vModelId) {}
    };

    final class PrometheusMetrics implements Metrics {
        private static final Logger logger = LogManager.getLogger(PrometheusMetrics.class);

        //TODO shorten these and/or make configurable by service
        // Could at least have the load time ones calibrated based on the provided loading timeout
        private static final double[] MS_TIME_BUCKETS = {
                .5, 1, 2, 5, 10, 20, 50, 75, 100, 200, 500, 1000, 2000,
                5000, 10000, 20000, 60000, 120000, 300000
        };

        private static final int INFO_METRICS_MAX = 5;

        private final CollectorRegistry registry;
        private final NettyServer metricServer;
        private final boolean shortNames;
        private final boolean enablePerModelMetrics;
        private final EnumMap<Metric, Collector> metricsMap = new EnumMap<>(Metric.class);

        public PrometheusMetrics(Map<String, String> params, Map<String, String> infoMetricParams) throws Exception {
            int port = 2112;
            boolean shortNames = true;
            boolean https = true;
            boolean enablePerModelMetrics = true;
            String memMetrics = "all"; // default to all
            for (Entry<String, String> ent : params.entrySet()) {
                switch (ent.getKey()) {
                case "port":
                    try {
                        port = Integer.parseInt(ent.getValue());
                    } catch (NumberFormatException nfe) {
                        throw new Exception("Invalid metrics port: " + ent.getValue());
                    }
                    break;
                case "per_model_metrics":
                    enablePerModelMetrics = "true".equalsIgnoreCase(ent.getValue());
                    break;
                case "fq_names":
                    shortNames = !"true".equalsIgnoreCase(ent.getValue());
                    break;
                case "scheme":
                    if ("http".equals(ent.getValue())) {
                        https = false;
                    } else if (!"https".equals(ent.getValue())) {
                        throw new Exception("Unsupported Prometheus metrics scheme " + ent.getValue()
                                            + " (must be either http or https)");
                    }
                    break;
                case "mem_detail":
                    memMetrics = ent.getValue();
                    break;
                default:
                    throw new Exception("Unrecognized metrics config parameter: " + ent.getKey());
                }
            }
            this.enablePerModelMetrics = enablePerModelMetrics;

            registry = new CollectorRegistry();
            for (Metric m : Metric.values()) {
                @SuppressWarnings("rawtypes")
                SimpleCollector.Builder builder;
                switch (m.type) {
                case COUNTER:
                    builder = Counter.build();
                    break;
                case GAUGE:
                    builder = Gauge.build();
                    break;
                case TIMING_HISTO:
                    builder = Histogram.build().buckets(MS_TIME_BUCKETS);
                    break;
                case AGE_HISTO:
                    builder = Histogram.build().buckets(ms(30, SECONDS), ms(5, MINUTES), ms(15, MINUTES),
                            ms(1, HOURS), ms(4, HOURS), ms(12, HOURS), ms(1, DAYS), ms(7, DAYS), ms(30, DAYS),
                            ms(90, DAYS));
                    break;
                case MODEL_SIZE_HISTO:
                    builder = Histogram.build().exponentialBuckets(256 * 1024, 4, 8); // 256KiB
                    break;
                case MESSAGE_SIZE_HISTO:
                    builder = Histogram.build().exponentialBuckets(128, 4, 8); // 128B
                    break;
                default:
                    // e.g. COUNTER_WITH_HISTO -- don't register this
                    continue;
                }

                if (m == API_REQUEST_TIME || m == API_REQUEST_COUNT || m == INVOKE_MODEL_TIME
                        || m == INVOKE_MODEL_COUNT || m == REQUEST_PAYLOAD_SIZE || m == RESPONSE_PAYLOAD_SIZE) {
                    if (this.enablePerModelMetrics && m.type != COUNTER_WITH_HISTO) {
                        builder.labelNames("method", "code", "modelId");
                    } else {
                        builder.labelNames("method", "code");
                    }
                } else if (this.enablePerModelMetrics && m.type != GAUGE && m.type != COUNTER && m.type != COUNTER_WITH_HISTO) {
                    builder.labelNames("modelId");
                }
                Collector collector = builder.name(m.promName).help(m.description).create();
                metricsMap.put(m, collector);
                if (!m.global) {
                    registry.register(collector);
                }
            }

            if (infoMetricParams != null && !infoMetricParams.isEmpty()){
                if (infoMetricParams.size() > INFO_METRICS_MAX) {
                    throw new Exception("Too many info metrics provided in env var " + MMESH_CUSTOM_ENV_VAR + ": \""
                            + infoMetricParams+ "\". The max is " + INFO_METRICS_MAX);
                }

                String metric_name = infoMetricParams.remove("metric_name");
                String[] labelNames = infoMetricParams.keySet().toArray(String[]::new);
                String[] labelValues = Stream.of(labelNames).map(infoMetricParams::get).toArray(String[]::new);
                Gauge infoMetricsGauge = Gauge.build()
                        .name(metric_name)
                        .help("Info Metrics")
                        .labelNames(labelNames)
                        .create();
                infoMetricsGauge.labels(labelValues).set(1.0);
                registry.register(infoMetricsGauge);
            }

            this.metricServer = new NettyServer(registry, port, https);
            this.shortNames = shortNames;
            logger.info("Will expose " + (https ? "https" : "http") + " Prometheus metrics on port " + port
                        + " using " + (shortNames ? "short" : "fully-qualified") + " method names");

            if (memMetrics != null) {
                registerMemoryExporters(registry, memMetrics);
            }
        }

        private static void registerMemoryExporters(CollectorRegistry registry, String value) {
            if ("none".equals(value)) {
                return;
            }
            if ("all".equals(value)) {
                value = "netty,gc,bp,mp,ma";
            }
            Set<String> allTypes = new HashSet<>(), validTypes = new HashSet<>();
            for (String type : value.split(",")) {
                allTypes.add(type.trim());
            }
            if (allTypes.remove("netty")) {
                new NettyMemoryExports().register(registry);
                validTypes.add("netty");
            }
            if (allTypes.remove("gc")) {
                new GarbageCollectorExports().register(registry);
                validTypes.add("gc");
            }
            if (allTypes.remove("bp")) {
                new BufferPoolsExports().register(registry);
                validTypes.add("bp");
            }
            if (allTypes.remove("ma")) {
                new MemoryAllocationExports().register(registry);
                validTypes.add("ma");
            }
            if (allTypes.remove("mp")) {
                new MemoryPoolsExports().register(registry);
                validTypes.add("mp");
            }
            if (!allTypes.isEmpty()) {
                logger.warn("Unrecognized memory detail metric type(s) specified: " + allTypes);
            }
            if (!validTypes.isEmpty()) {
                logger.info("Detailed memory usage metrics will be exposed: " + validTypes);
            }
        }

        @Override
        public void registerGlobals() {
            for (Entry<Metric, Collector> ent : metricsMap.entrySet()) {
                try {
                    if (ent.getKey().global) {
                        registry.register(ent.getValue());
                    }
                } catch (RuntimeException ise) {
                    logger.warn("Exception while registering global metric " + ent.getKey().name() + ": " + ise);
                }
            }
        }

        @Override
        public void unregisterGlobals() {
            for (Entry<Metric, Collector> ent : metricsMap.entrySet()) {
                try {
                    if (ent.getKey().global) {
                        registry.unregister(ent.getValue());
                    }
                } catch (RuntimeException ise) {
                    logger.warn("Exception while unregistering global metric " + ent.getKey().name() + ": " + ise);
                }
            }
        }

        @Override
        public void close() {
            this.metricServer.close();
        }

        @Override
        public boolean isPerModelMetricsEnabled() {
            return enablePerModelMetrics;
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public void logTimingMetricSince(Metric metric, long prevTime, boolean isNano) {
            Histogram timingMetric = (Histogram) metricsMap.get(metric);
            timingMetric.observe(isNano ? (System.nanoTime() - prevTime) / M : System.currentTimeMillis() - prevTime);
        }

        @Override
        public void logTimingMetricDuration(Metric metric, long elapsed, boolean isNano, String modelId) {
            if (enablePerModelMetrics) {
                ((Histogram) metricsMap.get(metric)).labels(modelId).observe(isNano ? elapsed / M : elapsed);
            } else {
                ((Histogram) metricsMap.get(metric)).observe(isNano ? elapsed / M : elapsed);
            }
        }

        @Override
        public void logSizeEventMetric(Metric metric, long value, String modelId) {
            if (enablePerModelMetrics) {
                ((Histogram) metricsMap.get(metric)).labels(modelId).observe(value * metric.newMultiplier);
            } else {
                ((Histogram) metricsMap.get(metric)).observe(value * metric.newMultiplier);
            }
        }

        @Override
        public void logGaugeMetric(Metric metric, long value) {
            ((Gauge) metricsMap.get(metric)).set(value * metric.newMultiplier);
        }

        @Override
        public void logCounterMetric(Metric metric) {
            if (metric.type == MetricType.COUNTER) {
                ((Counter) metricsMap.get(metric)).inc();
            }
        }

        @Override
        public void logRequestMetrics(boolean external, String name, long elapsedNanos, Code code,
                                      int reqPayloadSize, int respPayloadSize, String modelId, String vModelId) {
            final long elapsedMillis = elapsedNanos / M;
            final Histogram timingHisto = (Histogram) metricsMap
                    .get(external ? API_REQUEST_TIME : INVOKE_MODEL_TIME);
            String mId = vModelId == null ? modelId : vModelId;
            int idx = shortNames ? name.indexOf('/') : -1;
            String methodName = idx == -1 ? name : name.substring(idx + 1);
            if (enablePerModelMetrics) {
                timingHisto.labels(methodName, code.name(), mId).observe(elapsedMillis);
            } else {
                timingHisto.labels(methodName, code.name()).observe(elapsedMillis);
            }
            if (reqPayloadSize != -1) {
                if (enablePerModelMetrics) {
                    ((Histogram) metricsMap.get(REQUEST_PAYLOAD_SIZE))
                            .labels(methodName, code.name(), mId).observe(reqPayloadSize);
                } else {
                    ((Histogram) metricsMap.get(REQUEST_PAYLOAD_SIZE))
                            .labels(methodName, code.name()).observe(reqPayloadSize);
                }
            }
            if (respPayloadSize != -1) {
                if (enablePerModelMetrics) {
                    ((Histogram) metricsMap.get(RESPONSE_PAYLOAD_SIZE))
                            .labels(methodName, code.name(), mId).observe(respPayloadSize);
                } else {
                    ((Histogram) metricsMap.get(RESPONSE_PAYLOAD_SIZE))
                            .labels(methodName, code.name()).observe(respPayloadSize);
                }
            }
        }

        private static long ms(long duration, TimeUnit unit) {
            return MILLISECONDS.convert(duration, unit);
        }
    }

    final class StatsDMetrics implements Metrics {
        private static final Logger logger = LogManager.getLogger(StatsDMetrics.class);

        private final StatsDClient client;
        private final boolean legacy;
        private final boolean shortNames;

        public StatsDMetrics(Map<String, String> params) throws Exception {
            int port = 8126;
            boolean legacy = false;
            boolean shortNames = true;
            for (Entry<String, String> ent : params.entrySet()) {
                switch (ent.getKey()) {
                case "port":
                    try {
                        port = Integer.parseInt(ent.getValue());
                    } catch (NumberFormatException nfe) {
                        throw new Exception("Invalid metrics port: " + ent.getValue());
                    }
                    break;
                case "legacy":
                    legacy = "true".equalsIgnoreCase(ent.getValue());
                    break;
                case "fq_names":
                    shortNames = !"true".equalsIgnoreCase(ent.getValue());
                    break;
                default:
                    throw new Exception("Unrecognized metrics config parameter: " + ent.getKey());
                }
            }
            String prefix = legacy ? "model-mesh" : null;
            final boolean tagsInName = !legacy;
            this.client = new NonBlockingStatsDClient(prefix, "localhost", port, tagsInName) {
                @Override
                protected StatsDSender createSender(Callable<SocketAddress> addressLookup, int queueSize,
                        StatsDClientErrorHandler handler, DatagramChannel clientChannel, int maxPacketSizeBytes) {
                    return new StatsDSender(addressLookup, new LinkedBlockingQueue<>(queueSize), handler, clientChannel,
                            maxPacketSizeBytes, tagsInName);
                }
            };
            this.legacy = legacy;
            this.shortNames = shortNames;

            logger.info("Will emit " + (legacy ? "legacy" : "Sysdig") + " StatsD metrics to port " + port + " using "
                        + (shortNames ? "short" : "fully-qualified") + " method names");
        }

        @Override
        public boolean isPerModelMetricsEnabled() {
            return false;
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public void close() {
            client.close();
        }

        @Override
        public void logTimingMetricSince(Metric metric, long prevTime, boolean isNano) {
            client.recordExecutionTime(name(metric),
                    isNano ? (System.nanoTime() - prevTime) / M : System.currentTimeMillis() - prevTime);
        }

        @Override
        public void logTimingMetricDuration(Metric metric, long elapsed, boolean isNano, String modelId) {
            client.recordExecutionTime(name(metric), isNano ? elapsed / M : elapsed);
        }

        @Override
        public void logSizeEventMetric(Metric metric, long value, String modelId) {
            if (!legacy) {
                value *= metric.newMultiplier;
            }
            client.recordExecutionTime(name(metric), value);
        }

        @Override
        public void logGaugeMetric(Metric metric, long value) {
            if (!legacy) {
                value *= metric.newMultiplier;
            }
            client.recordGaugeValue(name(metric), value);
        }

        @Override
        public void logCounterMetric(Metric metric) {
            client.incrementCounter(name(metric));
        }

        private static volatile Map<String, String[]> okTagsCache = Collections.emptyMap(); // copy-on-write

        static String[] getOkTags(String method, boolean shortName) {
            Map<String, String[]> map = okTagsCache;
            String[] tags = map.get(method);
            if (tags == null) {
                int idx = shortName ? method.indexOf('/') : -1;
                String val = idx == -1 ? method : method.substring(idx + 1);
                tags = new String[] { "method=" + val.replace('/', '.'), "code=" + Code.OK.name() };
                map = new HashMap<>(map);
                map.put(method, tags);
                okTagsCache = map; // racy but no biggie
            }
            return tags;
        }

        @Override
        public void logRequestMetrics(boolean external, String name, long elapsedNanos, Code code,
                                      int reqPayloadSize, int respPayloadSize, String modelId, String vModelId) {
            final StatsDClient client = this.client;
            final long elapsedMillis = elapsedNanos / M;
            final String countName = name(external ? API_REQUEST_COUNT : INVOKE_MODEL_COUNT);
            if (legacy) {
                StringBuilder sb = new StringBuilder();
                int idx = shortNames ? name.indexOf('/') : -1;
                if (!shortNames) {
                    name = name.replace('/', '.');
                }
                if (code == Code.OK) {
                    String timeName = name(external ? API_REQUEST_TIME : INVOKE_MODEL_TIME);
                    // ship response time for successful calls
                    appendName(sb, timeName, name, idx);
                    client.recordExecutionTime(sb.toString(), elapsedMillis);
                    if (timeName != countName) {
                        sb.setLength(0);
                    }
                }
                if (sb.length() == 0) {
                    appendName(sb, countName, name, idx);
                }
                // ship counter for all calls
                client.incrementCounter(sb.append('.').append(code.name()).toString());
                if (reqPayloadSize != -1) {
                    sb.setLength(0);
                    client.recordExecutionTime(appendName(sb, name(REQUEST_PAYLOAD_SIZE), name, idx)
                            .append('.').append(code.name()).toString(), reqPayloadSize);
                }
                if (respPayloadSize != -1) {
                    sb.setLength(0);
                    client.recordExecutionTime(appendName(sb, name(RESPONSE_PAYLOAD_SIZE), name, idx)
                            .append('.').append(code.name()).toString(), respPayloadSize);
                }
            } else {
                String[] tags = getOkTags(name, shortNames);
                if (code == Code.OK) {
                    String timeName = name(external ? API_REQUEST_TIME : INVOKE_MODEL_TIME);
                    // ship response time for successful calls
                    client.recordExecutionTime(timeName, elapsedMillis, tags);
                } else {
                    tags = new String[] { tags[0], "code=" + code.name() };
                }
                // ship counter for all calls
                client.incrementCounter(countName, tags);
                if (reqPayloadSize != -1) {
                    client.recordExecutionTime(name(REQUEST_PAYLOAD_SIZE), reqPayloadSize, tags);
                }
                if (respPayloadSize != -1) {
                    client.recordExecutionTime(name(RESPONSE_PAYLOAD_SIZE), respPayloadSize, tags);
                }
            }
        }

        private StringBuilder appendName(StringBuilder sb, String metricName, String methodName, int idx) {
            sb.append(metricName);
            if (idx == -1) {
                sb.append(methodName);
            } else {
                sb.append(methodName, idx + 1, methodName.length());
            }
            return sb;
        }

        private String name(Metric metric) {
            return legacy ? metric.oldStatsdName : metric.newStatsdName;
        }
    }
}
