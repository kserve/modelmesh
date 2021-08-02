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

/*
 * This is a modified (optimized) version of the equivalent class from the java-dogstatsd-client
 * https://github.com/DataDog/java-dogstatsd-client, which is also a dependency.
 *
 * Note the current mods are based on version 2.7 of the client, and need rebasing on later
 * versions (2.8).
 */
package com.ibm.watson.statsd;

//import jnr.unixsocket.UnixDatagramChannel;
//import jnr.unixsocket.UnixSocketAddress;
//import jnr.unixsocket.UnixSocketOptions;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.DatagramChannel;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.ibm.watson.statsd.StatsDSender.Message;
import com.timgroup.statsd.Event;
import com.timgroup.statsd.ServiceCheck;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.StatsDClientErrorHandler;
import com.timgroup.statsd.StatsDClientException;


/**
 * A simple StatsD client implementation facilitating metrics recording.
 *
 * <p>Upon instantiation, this client will establish a socket connection to a StatsD instance
 * running on the specified host and port. Metrics are then sent over this connection as they are
 * received by the client.
 * </p>
 *
 * <p>Three key methods are provided for the submission of data-points for the application under
 * scrutiny:
 * <ul>
 *   <li>{@link #incrementCounter} - adds one to the value of the specified named counter</li>
 *   <li>{@link #recordGaugeValue} - records the latest fixed value for the specified named gauge</li>
 *   <li>{@link #recordExecutionTime} - records an execution time in milliseconds for the specified named operation</li>
 *   <li>{@link #recordHistogramValue} - records a value, to be tracked with average, maximum, and percentiles</li>
 *   <li>{@link #recordEvent} - records an event</li>
 *   <li>{@link #recordSetValue} - records a value in a set</li>
 * </ul>
 * From the perspective of the application, these methods are non-blocking, with the resulting
 * IO operations being carried out in a separate thread. Furthermore, these methods are guaranteed
 * not to throw an exception which may disrupt application execution.
 *
 * <p>As part of a clean system shutdown, the {@link #stop()} method should be invoked
 * on any StatsD clients.</p>
 *
 * @author Tom Denley
 *
 */
public class NonBlockingStatsDClient implements StatsDClient {

    /**
     * 1400 chosen as default here so that the number of bytes in a message plus the number of bytes required
     * for additional udp headers should be under the 1500 Maximum Transmission Unit for ethernet.
     * See https://github.com/DataDog/java-dogstatsd-client/pull/17 for discussion.
     */
    private static final int DEFAULT_MAX_PACKET_SIZE_BYTES = 1400;
    private static final int SOCKET_TIMEOUT_MS = 100;
    private static final int SOCKET_BUFFER_BYTES = -1;

    private static final StatsDClientErrorHandler NO_OP_HANDLER = new StatsDClientErrorHandler() {
        @Override public void handle(final Exception e) { /* No-op */ }
    };

    /**
     * The NumberFormat instances are not threadsafe but are only ever called from
     * the sender thread.
     */
    private static final NumberFormat NUMBER_FORMATTER = newFormatter();
    private static final NumberFormat SAMPLE_RATE_FORMATTER = newFormatter();

    static {
        NUMBER_FORMATTER.setMaximumFractionDigits(6);
        SAMPLE_RATE_FORMATTER.setMinimumFractionDigits(6);
    }

    private static NumberFormat newFormatter() {
        // Always create the formatter for the US locale in order to avoid this bug:
        // https://github.com/indeedeng/java-dogstatsd-client/issues/3
        NumberFormat numberFormatter = NumberFormat.getInstance(Locale.US);
        numberFormatter.setGroupingUsed(false);

        // we need to specify a value for Double.NaN that is recognized by dogStatsD
        if (numberFormatter instanceof DecimalFormat) { // better safe than a runtime error
            final DecimalFormat decimalFormat = (DecimalFormat) numberFormatter;
            final DecimalFormatSymbols symbols = decimalFormat.getDecimalFormatSymbols();
            symbols.setNaN("NaN");
            decimalFormat.setDecimalFormatSymbols(symbols);
        }
        return numberFormatter;
    }

    private final String prefix;
    private final DatagramChannel clientChannel;
    private final StatsDClientErrorHandler handler;
    private final String constantTagsRendered;

    // If true tags will be serialized as part of the name instead of at the end
    private final boolean tagsInName;

    private final ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
        final ThreadFactory delegate = Executors.defaultThreadFactory();
        @Override public Thread newThread(final Runnable r) {
            final Thread result = delegate.newThread(r);
            result.setName("StatsD-" + result.getName());
            result.setDaemon(true);
            return result;
        }
    });

    private final StatsDSender statsDSender;

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are consumed, guaranteeing
     * that failures in metrics will not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public NonBlockingStatsDClient(final String prefix, final String hostname, final int port, final boolean tagsInName) throws StatsDClientException {
        this(prefix, Integer.MAX_VALUE, null, tagsInName, null, staticStatsDAddressResolution(hostname, port),
                SOCKET_TIMEOUT_MS, SOCKET_BUFFER_BYTES, DEFAULT_MAX_PACKET_SIZE_BYTES);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are consumed, guaranteeing
     * that failures in metrics will not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public NonBlockingStatsDClient(final String prefix, final String hostname, final int port) throws StatsDClientException {
        this(prefix, hostname, port, Integer.MAX_VALUE);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are consumed, guaranteeing
     * that failures in metrics will not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     * @param queueSize
     *     the maximum amount of unprocessed messages in the BlockingQueue.
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public NonBlockingStatsDClient(final String prefix, final String hostname, final int port, final int queueSize) throws StatsDClientException {
        this(prefix, hostname, port, queueSize, null, null);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are consumed, guaranteeing
     * that failures in metrics will not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     * @param constantTags
     *     tags to be added to all content sent
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public NonBlockingStatsDClient(final String prefix, final String hostname, final int port, final String... constantTags) throws StatsDClientException {
        this(prefix, hostname, port, Integer.MAX_VALUE, constantTags, null);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are consumed, guaranteeing
     * that failures in metrics will not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     * @param constantTags
     *     tags to be added to all content sent
     * @param maxPacketSizeBytes
     *     the maximum number of bytes for a message that can be sent
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public NonBlockingStatsDClient(final String prefix, final String hostname, final int port, final String[] constantTags, final int maxPacketSizeBytes) throws StatsDClientException {
        this(prefix, hostname, port, Integer.MAX_VALUE, constantTags, null, maxPacketSizeBytes);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are consumed, guaranteeing
     * that failures in metrics will not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     * @param constantTags
     *     tags to be added to all content sent
     * @param queueSize
     *     the maximum amount of unprocessed messages in the BlockingQueue.
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public NonBlockingStatsDClient(final String prefix, final String hostname, final int port, final int queueSize, final String... constantTags) throws StatsDClientException {
        this(prefix, hostname, port, queueSize, constantTags, null);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are passed to the specified
     * handler and then consumed, guaranteeing that failures in metrics will
     * not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     * @param constantTags
     *     tags to be added to all content sent
     * @param errorHandler
     *     handler to use when an exception occurs during usage, may be null to indicate noop
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public NonBlockingStatsDClient(final String prefix, final String hostname, final int port,
                                   final String[] constantTags, final StatsDClientErrorHandler errorHandler) throws StatsDClientException {
        this(prefix, Integer.MAX_VALUE, constantTags, errorHandler, staticStatsDAddressResolution(hostname, port), SOCKET_TIMEOUT_MS, SOCKET_BUFFER_BYTES, DEFAULT_MAX_PACKET_SIZE_BYTES);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are passed to the specified
     * handler and then consumed, guaranteeing that failures in metrics will
     * not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     * @param constantTags
     *     tags to be added to all content sent
     * @param errorHandler
     *     handler to use when an exception occurs during usage, may be null to indicate noop
     * @param queueSize
     *     the maximum amount of unprocessed messages in the BlockingQueue.
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public NonBlockingStatsDClient(final String prefix, final String hostname, final int port, final int queueSize,
                                   final String[] constantTags, final StatsDClientErrorHandler errorHandler) throws StatsDClientException {
        this(prefix, queueSize, constantTags, errorHandler, staticStatsDAddressResolution(hostname, port), SOCKET_TIMEOUT_MS, SOCKET_BUFFER_BYTES, DEFAULT_MAX_PACKET_SIZE_BYTES);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are passed to the specified
     * handler and then consumed, guaranteeing that failures in metrics will
     * not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     * @param constantTags
     *     tags to be added to all content sent
     * @param errorHandler
     *     handler to use when an exception occurs during usage, may be null to indicate noop
     * @param queueSize
     *     the maximum amount of unprocessed messages in the BlockingQueue.
     * @param maxPacketSizeBytes
     *     the maximum number of bytes for a message that can be sent
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public NonBlockingStatsDClient(final String prefix, final String hostname, final int port, final int queueSize,
                                   final String[] constantTags, final StatsDClientErrorHandler errorHandler, final int maxPacketSizeBytes) throws StatsDClientException {
        this(prefix, queueSize, constantTags, errorHandler, staticStatsDAddressResolution(hostname, port), SOCKET_TIMEOUT_MS, SOCKET_BUFFER_BYTES, maxPacketSizeBytes);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are passed to the specified
     * handler and then consumed, guaranteeing that failures in metrics will
     * not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     * @param constantTags
     *     tags to be added to all content sent
     * @param errorHandler
     *     handler to use when an exception occurs during usage, may be null to indicate noop
     * @param queueSize
     *     the maximum amount of unprocessed messages in the BlockingQueue.
     * @param timeout
     *     the timeout in milliseconds for blocking operations. Applies to unix sockets only.
     * @param bufferSize
     *     the socket buffer size in bytes. Applies to unix sockets only.
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public NonBlockingStatsDClient(final String prefix, final String hostname, final int port, final int queueSize, int timeout, int bufferSize,
                                   final String[] constantTags, final StatsDClientErrorHandler errorHandler) throws StatsDClientException {
        this(prefix, queueSize, constantTags, errorHandler, staticStatsDAddressResolution(hostname, port), timeout, bufferSize);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are passed to the specified
     * handler and then consumed, guaranteeing that failures in metrics will
     * not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param constantTags
     *     tags to be added to all content sent
     * @param errorHandler
     *     handler to use when an exception occurs during usage, may be null to indicate noop
     * @param addressLookup
     *     yields the IP address and socket of the StatsD server
     * @param queueSize
     *     the maximum amount of unprocessed messages in the BlockingQueue.
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public NonBlockingStatsDClient(final String prefix,  final int queueSize, String[] constantTags, final StatsDClientErrorHandler errorHandler,
                                   final Callable<SocketAddress> addressLookup) throws StatsDClientException {
        this(prefix, queueSize, constantTags, errorHandler, addressLookup, SOCKET_TIMEOUT_MS, SOCKET_BUFFER_BYTES, DEFAULT_MAX_PACKET_SIZE_BYTES);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are passed to the specified
     * handler and then consumed, guaranteeing that failures in metrics will
     * not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param constantTags
     *     tags to be added to all content sent
     * @param errorHandler
     *     handler to use when an exception occurs during usage, may be null to indicate noop
     * @param addressLookup
     *     yields the IP address and socket of the StatsD server
     * @param queueSize
     *     the maximum amount of unprocessed messages in the BlockingQueue.
     * @param timeout
     *     the timeout in milliseconds for blocking operations. Applies to unix sockets only.
     * @param bufferSize
     *     the socket buffer size in bytes. Applies to unix sockets only.
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public NonBlockingStatsDClient(final String prefix,  final int queueSize, String[] constantTags, final StatsDClientErrorHandler errorHandler,
                                   final Callable<SocketAddress> addressLookup, final int timeout, final int bufferSize) throws StatsDClientException {
        this(prefix, queueSize, constantTags, errorHandler, addressLookup, timeout, bufferSize, DEFAULT_MAX_PACKET_SIZE_BYTES);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are passed to the specified
     * handler and then consumed, guaranteeing that failures in metrics will
     * not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param constantTags
     *     tags to be added to all content sent
     * @param errorHandler
     *     handler to use when an exception occurs during usage, may be null to indicate noop
     * @param addressLookup
     *     yields the IP address and socket of the StatsD server
     * @param queueSize
     *     the maximum amount of unprocessed messages in the BlockingQueue.
     * @param timeout
     *     the timeout in milliseconds for blocking operations. Applies to unix sockets only.
     * @param bufferSize
     *     the socket buffer size in bytes. Applies to unix sockets only.
     * @param maxPacketSizeBytes
     *     the maximum number of bytes for a message that can be sent
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public NonBlockingStatsDClient(final String prefix,  final int queueSize, String[] constantTags, final StatsDClientErrorHandler errorHandler,
                                   final Callable<SocketAddress> addressLookup, final int timeout, final int bufferSize, final int maxPacketSizeBytes) throws StatsDClientException {
       this(prefix, queueSize, constantTags, false, errorHandler, addressLookup, timeout, bufferSize, maxPacketSizeBytes);
    }

    public NonBlockingStatsDClient(final String prefix,  final int queueSize, String[] constantTags, boolean tagsInName, final StatsDClientErrorHandler errorHandler,
                                   final Callable<SocketAddress> addressLookup, final int timeout, final int bufferSize, final int maxPacketSizeBytes) throws StatsDClientException {
        if((prefix != null) && (!prefix.isEmpty())) {
            this.prefix = prefix + ".";
        } else {
            this.prefix = "";
        }
        if(errorHandler == null) {
            handler = NO_OP_HANDLER;
        }
        else {
            handler = errorHandler;
        }

        /* Empty list should be null for faster comparison */
        if((constantTags != null) && (constantTags.length == 0)) {
            constantTags = null;
        }

        if(constantTags != null) {
            constantTagsRendered = tagString(constantTags, null,
                    !tagsInName, new StringBuilder()).toString();
        } else {
            constantTagsRendered = null;
        }

        this.tagsInName = tagsInName;

        try {
//            final SocketAddress address = addressLookup.call();
//            if (address instanceof UnixSocketAddress) {
//                clientChannel = UnixDatagramChannel.open();
//                // Set send timeout, to handle the case where the transmission buffer is full
//                // If no timeout is set, the send becomes blocking
//                if (timeout > 0) {
//                    clientChannel.setOption(UnixSocketOptions.SO_SNDTIMEO, timeout);
//                }
//                if (bufferSize > 0) {
//                    clientChannel.setOption(UnixSocketOptions.SO_SNDBUF, bufferSize);
//                }
//            } else {
                clientChannel = DatagramChannel.open();
//            }

             if (addressLookup instanceof StaticAddressResolution) {
                 clientChannel.connect(addressLookup.call());
             }
        } catch (final Exception e) {
            throw new StatsDClientException("Failed to start StatsD client", e);
        }
        statsDSender = createSender(addressLookup, queueSize, handler, clientChannel, maxPacketSizeBytes);
        executor.submit(statsDSender);
    }

    protected StatsDSender createSender(final Callable<SocketAddress> addressLookup, final int queueSize,
                                        final StatsDClientErrorHandler handler, final DatagramChannel clientChannel, final int maxPacketSizeBytes) {
        return new StatsDSender(addressLookup, queueSize, handler, clientChannel, maxPacketSizeBytes);
    }

    /**
     * Cleanly shut down this StatsD client. This method may throw an exception if
     * the socket cannot be closed.
     */
    @Override
    public void stop() {
        try {
            statsDSender.shutdown();
            executor.shutdown();
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
                if (!executor.isTerminated()) {
                    executor.shutdownNow();
                }
            } catch (Exception e) {
                handler.handle(e);
                if (!executor.isTerminated()) {
                    executor.shutdownNow();
                }
            }
        }
        catch (final Exception e) {
            handler.handle(e);
        }
        finally {
            if (clientChannel != null) {
                try {
                    clientChannel.close();
                }
                catch (final IOException e) {
                    handler.handle(e);
                }
            }
        }
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * Generate a suffix conveying the given tag list to the client
     */
    static StringBuilder tagString(final String[] tags, final String tagPrefix,
            final boolean withPipe, final StringBuilder sb) {
        if(tagPrefix != null) {
            sb.append(tagPrefix);
            if((tags == null) || (tags.length == 0)) {
                return sb;
            }
            sb.append(',');
        } else {
            if((tags == null) || (tags.length == 0)) {
                return sb;
            }
            if (withPipe) {
                sb.append('|');
            }
            sb.append('#');
        }

        for(int n=tags.length - 1; n>=0; n--) {
            sb.append(tags[n]);
            if(n > 0) {
                sb.append(',');
            }
        }
        return sb;
    }

    /**
     * Generate a suffix conveying the given tag list to the client
     */
    StringBuilder tagString(final String[] tags, StringBuilder builder) {
        return tagString(tags, constantTagsRendered, !tagsInName, builder);
    }

    abstract class StatsDMessage implements Message {
        final String aspect;
        final String type;
        final double sampleRate; // NaN for none
        final String[] tags;

        protected StatsDMessage(String aspect, String type, double sampleRate, String[] tags) {
            this.aspect = aspect;
            this.type = type;
            this.sampleRate = sampleRate;
            this.tags = tags;
        }

        @Override
        public final void writeTo(StringBuilder builder) {
            builder.append(prefix).append(aspect);
            if (tagsInName) {
                tagString(tags, builder);
            }
            writeValue(builder.append(':'));
            builder.append('|').append(type);
            if (!Double.isNaN(sampleRate)) {
                builder.append('|').append('@').append(SAMPLE_RATE_FORMATTER.format(sampleRate));
            }
            if (!tagsInName) {
                tagString(tags, builder);
            }
        }

        protected abstract void writeValue(StringBuilder builder);
    }

    // send double with sample rate
    private void send(String aspect, final double value, String type, double sampleRate, String[] tags) {
        if(Double.isNaN(sampleRate) || !isInvalidSample(sampleRate)) {
            statsDSender.send(new StatsDMessage(aspect, type, sampleRate, tags) {
                @Override protected void writeValue(StringBuilder builder) {
                    builder.append(NUMBER_FORMATTER.format(value));
                }
            });
        }
    }

    // send double without sample rate
    private void send(String aspect, final double value, String type, String[] tags) {
        send(aspect, value, type, Double.NaN, tags);
    }

    // send long with sample rate
    private void send(String aspect, final long value, String type, double sampleRate, String[] tags) {
        if(Double.isNaN(sampleRate) || !isInvalidSample(sampleRate)) {
            statsDSender.send(new StatsDMessage(aspect, type, sampleRate, tags) {
                @Override protected void writeValue(StringBuilder builder) {
                    builder.append(value);
                }
            });
        }
    }

    // send long without sample rate
    private void send(String aspect, final long value, String type, String[] tags) {
        send(aspect, value, type, Double.NaN, tags);
    }

    /**
     * Adjusts the specified counter by a given delta.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the counter to adjust
     * @param delta
     *     the amount to adjust the counter by
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void count(final String aspect, final long delta, final String... tags) {
        send(aspect, delta, "c", tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void count(final String aspect, final long delta, final double sampleRate, final String...tags) {
        send(aspect, delta, "c", sampleRate, tags);
    }

    /**
     * Adjusts the specified counter by a given delta.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the counter to adjust
     * @param delta
     *     the amount to adjust the counter by
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void count(final String aspect, final double delta, final String... tags) {
        send(aspect, delta, "c", tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void count(final String aspect, final double delta, final double sampleRate, final String...tags) {
        send(aspect, delta, "c", sampleRate, tags);
    }

    /**
     * Increments the specified counter by one.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the counter to increment
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void incrementCounter(final String aspect, final String... tags) {
        count(aspect, 1L, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrementCounter(final String aspect, final double sampleRate, final String... tags) {
    	count(aspect, 1, sampleRate, tags);
    }

    /**
     * Convenience method equivalent to {@link #incrementCounter(String, String[])}.
     */
    @Override
    public void increment(final String aspect, final String... tags) {
        incrementCounter(aspect, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void increment(final String aspect, final double sampleRate, final String...tags ) {
    	incrementCounter(aspect, sampleRate, tags);
    }

    /**
     * Decrements the specified counter by one.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the counter to decrement
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void decrementCounter(final String aspect, final String... tags) {
        count(aspect, -1, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void decrementCounter(String aspect, final double sampleRate, final String... tags) {
        count(aspect, -1, sampleRate, tags);
    }

    /**
     * Convenience method equivalent to {@link #decrementCounter(String, String[])}.
     */
    @Override
    public void decrement(final String aspect, final String... tags) {
        decrementCounter(aspect, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void decrement(final String aspect, final double sampleRate, final String... tags) {
        decrementCounter(aspect, sampleRate, tags);
    }

    /**
     * Records the latest fixed value for the specified named gauge.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the gauge
     * @param value
     *     the new reading of the gauge
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordGaugeValue(final String aspect, final double value, final String... tags) {
        send(aspect, value, "g", tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordGaugeValue(final String aspect, final double value, final double sampleRate, final String... tags) {
        send(aspect, value, "g", sampleRate, tags);
    }

    /**
     * Convenience method equivalent to {@link #recordGaugeValue(String, double, String[])}.
     */
    @Override
    public void gauge(final String aspect, final double value, final String... tags) {
        recordGaugeValue(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void gauge(final String aspect, final double value, final double sampleRate, final String... tags) {
        recordGaugeValue(aspect, value, sampleRate, tags);
    }


    /**
     * Records the latest fixed value for the specified named gauge.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the gauge
     * @param value
     *     the new reading of the gauge
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordGaugeValue(final String aspect, final long value, final String... tags) {
        send(aspect, value, "g", tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordGaugeValue(final String aspect, final long value, final double sampleRate, final String... tags) {
    	send(aspect, value, "g", sampleRate, tags);
    }

    /**
     * Convenience method equivalent to {@link #recordGaugeValue(String, long, String[])}.
     */
    @Override
    public void gauge(final String aspect, final long value, final String... tags) {
        recordGaugeValue(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void gauge(final String aspect, final long value, final double sampleRate, final String... tags) {
        recordGaugeValue(aspect, value, sampleRate, tags);
    }

    /**
     * Records an execution time in milliseconds for the specified named operation.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the timed operation
     * @param timeInMs
     *     the time in milliseconds
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordExecutionTime(final String aspect, final long timeInMs, final String... tags) {
        send(aspect, timeInMs, "ms", tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordExecutionTime(final String aspect, final long timeInMs, final double sampleRate, final String... tags) {
        send(aspect, timeInMs, "ms", sampleRate, tags);
    }

    /**
     * Convenience method equivalent to {@link #recordExecutionTime(String, long, String[])}.
     */
    @Override
    public void time(final String aspect, final long value, final String... tags) {
        recordExecutionTime(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void time(final String aspect, final long value, final double sampleRate, final String... tags) {
        recordExecutionTime(aspect, value, sampleRate, tags);
    }

    /**
     * Records a value for the specified named histogram.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the histogram
     * @param value
     *     the value to be incorporated in the histogram
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordHistogramValue(final String aspect, final double value, final String... tags) {
        send(aspect, value, "h", tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordHistogramValue(final String aspect, final double value, final double sampleRate, final String... tags) {
        send(aspect, value, "h", sampleRate, tags);
    }

    /**
     * Convenience method equivalent to {@link #recordHistogramValue(String, double, String[])}.
     */
    @Override
    public void histogram(final String aspect, final double value, final String... tags) {
        recordHistogramValue(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void histogram(final String aspect, final double value, final double sampleRate, final String... tags) {
        recordHistogramValue(aspect, value, sampleRate, tags);
    }

    /**
     * Records a value for the specified named histogram.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the histogram
     * @param value
     *     the value to be incorporated in the histogram
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordHistogramValue(final String aspect, final long value, final String... tags) {
        send(aspect, value, "h", tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordHistogramValue(final String aspect, final long value, final double sampleRate, final String... tags) {
        send(aspect, value, "h", sampleRate, tags);
    }

    /**
     * Convenience method equivalent to {@link #recordHistogramValue(String, long, String[])}.
     */
    @Override
    public void histogram(final String aspect, final long value, final String... tags) {
        recordHistogramValue(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void histogram(final String aspect, final long value, final double sampleRate, final String... tags) {
        recordHistogramValue(aspect, value, sampleRate, tags);
    }

     /**
     * Records a value for the specified named distribution.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * <p>This is a beta feature and must be enabled specifically for your organization.</p>
     *
     * @param aspect
     *     the name of the distribution
     * @param value
     *     the value to be incorporated in the distribution
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordDistributionValue(final String aspect, final double value, final String... tags) {
        send(aspect, value, "d", tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordDistributionValue(final String aspect, final double value, final double sampleRate, final String... tags) {
        send(aspect, value, "d", sampleRate, tags);
    }

    /**
     * Convenience method equivalent to {@link #recordDistributionValue(String, double, String[])}.
     */
    @Override
    public void distribution(final String aspect, final double value, final String... tags) {
        recordDistributionValue(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void distribution(final String aspect, final double value, final double sampleRate, final String... tags) {
        recordDistributionValue(aspect, value, sampleRate, tags);
    }
    /**
     * Records a value for the specified named distribution.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * <p>This is a beta feature and must be enabled specifically for your organization.</p>
     *
     * @param aspect
     *     the name of the distribution
     * @param value
     *     the value to be incorporated in the distribution
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordDistributionValue(final String aspect, final long value, final String... tags) {
        send(aspect, value, "d", tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordDistributionValue(final String aspect, final long value, final double sampleRate, final String... tags) {
        send(aspect, value, "d", sampleRate, tags);
    }

    /**
     * Convenience method equivalent to {@link #recordDistributionValue(String, long, String[])}.
     */
    @Override
    public void distribution(final String aspect, final long value, final String... tags) {
        recordDistributionValue(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void distribution(final String aspect, final long value, final double sampleRate, final String... tags) {
        recordDistributionValue(aspect, value, sampleRate, tags);
    }

    private StringBuilder eventMap(final Event event, StringBuilder res) {
        final long millisSinceEpoch = event.getMillisSinceEpoch();
        if (millisSinceEpoch != -1) {
            res.append("|d:").append(millisSinceEpoch / 1000);
        }

        final String hostname = event.getHostname();
        if (hostname != null) {
            res.append("|h:").append(hostname);
        }

        final String aggregationKey = event.getAggregationKey();
        if (aggregationKey != null) {
            res.append("|k:").append(aggregationKey);
        }

        final String priority = event.getPriority();
        if (priority != null) {
            res.append("|p:").append(priority);
        }

        final String alertType = event.getAlertType();
        if (alertType != null) {
            res.append("|t:").append(alertType);
        }

        return res;
    }

    /**
     * Records an event
     *
     * <p>This method is a DataDog extension, and may not work with other servers.</p>
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param event
     *     The event to record
     * @param tags
     *     array of tags to be added to the data
     *
     * @see <a href="http://docs.datadoghq.com/guides/dogstatsd/#events-1">http://docs.datadoghq.com/guides/dogstatsd/#events-1</a>
     */
    @Override
    public void recordEvent(final Event event, final String... tags) {
        statsDSender.send(new Message() {
            @Override public void writeTo(StringBuilder builder) {
                final String title = escapeEventString(prefix + event.getTitle());
                final String text = escapeEventString(event.getText());
                builder.append("_e{").append(title.length()).append(",").append(text.length()).append("}:").append(title)
                .append("|").append(text);
                eventMap(event, builder);
                tagString(tags, builder);
            }
        });
    }

    private static String escapeEventString(final String title) {
        return title.replace("\n", "\\n");
    }

    /**
     * Records a run status for the specified named service check.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.</p>
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param sc
     *     the service check object
     */
    @Override
    public void recordServiceCheckRun(final ServiceCheck sc) {
        statsDSender.send(new Message() {
            @Override public void writeTo(StringBuilder sb) {
                // see http://docs.datadoghq.com/guides/dogstatsd/#service-checks
                sb.append("_sc|").append(sc.getName()).append("|").append(sc.getStatus());
                if (sc.getTimestamp() > 0) {
                    sb.append("|d:").append(sc.getTimestamp());
                }
                if (sc.getHostname() != null) {
                    sb.append("|h:").append(sc.getHostname());
                }
                tagString(sc.getTags(), sb);
                if (sc.getMessage() != null) {
                    sb.append("|m:").append(sc.getEscapedMessage());
                }
            }
        });
    }

    /**
     * Convenience method equivalent to {@link #recordServiceCheckRun(ServiceCheck sc)}.
     */
    @Override
    public void serviceCheck(final ServiceCheck sc) {
        recordServiceCheckRun(sc);
    }


    /**
     * Records a value for the specified set.
     *
     * Sets are used to count the number of unique elements in a group. If you want to track the number of
     * unique visitor to your site, sets are a great way to do that.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.</p>
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the set
     * @param value
     *     the value to track
     * @param tags
     *     array of tags to be added to the data
     *
     * @see <a href="http://docs.datadoghq.com/guides/dogstatsd/#sets">http://docs.datadoghq.com/guides/dogstatsd/#sets</a>
     */
    @Override
    public void recordSetValue(final String aspect, final String value, final String... tags) {
        // documentation is light, but looking at dogstatsd source, we can send string values
        // here instead of numbers
        statsDSender.send(new StatsDMessage(aspect, "s", Double.NaN, tags) {
            @Override protected void writeValue(StringBuilder builder) {
                builder.append(value);
            }
        });
    }

    private boolean isInvalidSample(double sampleRate) {
        return sampleRate != 1 && ThreadLocalRandom.current().nextDouble() > sampleRate;
    }

    /**
     * Create dynamic lookup for the given host name and port.
     *
     * @param hostname the host name of the targeted StatsD server
     * @param port     the port of the targeted StatsD server
     * @return a function to perform the lookup
     */
    public static Callable<SocketAddress> volatileAddressResolution(final String hostname, final int port) {
        return new Callable<SocketAddress>() {
            @Override public SocketAddress call() throws UnknownHostException {
//                if (port == 0) { // Hostname is a file path to the socket
//                    return new UnixSocketAddress(hostname);
//                } else {
                    return new InetSocketAddress(InetAddress.getByName(hostname), port);
//                }
            }
        };
    }

    static class StaticAddressResolution implements Callable<SocketAddress> {
        private final SocketAddress address;
        public StaticAddressResolution(SocketAddress address) {
            this.address = address;
        }
        @Override
        public SocketAddress call() {
            return address;
        }
    }

  /**
   * Lookup the address for the given host name and cache the result.
   *
   * @param hostname the host name of the targeted StatsD server
   * @param port     the port of the targeted StatsD server
   * @return a function that cached the result of the lookup
   * @throws Exception if the lookup fails, i.e. {@link UnknownHostException}
   */
    public static Callable<SocketAddress> staticAddressResolution(final String hostname, final int port) throws Exception {
        final SocketAddress address = volatileAddressResolution(hostname, port).call();
        return new StaticAddressResolution(address);
    }

    private static Callable<SocketAddress> staticStatsDAddressResolution(final String hostname, final int port) throws StatsDClientException {
        try {
            return staticAddressResolution(hostname, port);
        } catch (final Exception e) {
            throw new StatsDClientException("Failed to lookup StatsD host", e);
        }
    }
}
