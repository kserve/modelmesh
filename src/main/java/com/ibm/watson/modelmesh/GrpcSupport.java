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

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.ibm.watson.litelinks.server.ReleaseAfterResponse;
import io.grpc.Context;
import io.grpc.Context.CancellationListener;
import io.grpc.Deadline;
import io.grpc.Drainable;
import io.grpc.InternalMetadata;
import io.grpc.KnownLength;
import io.grpc.Metadata;
import io.grpc.Metadata.BinaryMarshaller;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.internal.CompositeReadableBuffer;
import io.grpc.internal.GrpcUtil;
import io.grpc.internal.MessageFramer;
import io.grpc.internal.ReadableBuffer;
import io.grpc.internal.WritableBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledDirectByteBuf;
import io.netty.buffer.UnpooledHeapByteBuf;
import io.netty.buffer.UnpooledUnsafeDirectByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * Supporting methods for gRPC mechanics
 */
public final class GrpcSupport {

    private static final Logger logger = LoggerFactory.getLogger(GrpcSupport.class);

    private GrpcSupport() {} // static only

    private static final int MIN_ZERO_COPY_SIZE = 256; // don't attempt zero copy with small buffers

    static final BinaryMarshaller<String> UTF8_MARSHALLER = new BinaryMarshaller<>() {
        @Override
        public String parseBytes(byte[] serialized) {
            return new String(serialized, StandardCharsets.UTF_8);
        }

        @Override
        public byte[] toBytes(String value) {
            return value.getBytes(StandardCharsets.UTF_8);
        }
    };

    // visible-ascii (v)model-ids
    static final Metadata.Key<String> MODEL_ID_HEADER_KEY = Metadata.Key.of("mm-model-id", ASCII_STRING_MARSHALLER);
    static final Metadata.Key<String> VMODEL_ID_HEADER_KEY = Metadata.Key.of("mm-vmodel-id", ASCII_STRING_MARSHALLER);

    // utf8-encoded (v)model-ids
    static final Metadata.Key<String> MODEL_ID_BIN_HEADER_KEY = Metadata.Key.of("mm-model-id-bin", UTF8_MARSHALLER);
    static final Metadata.Key<String> VMODEL_ID_BIN_HEADER_KEY = Metadata.Key.of("mm-vmodel-id-bin", UTF8_MARSHALLER);

    static final Metadata.Key<byte[]> MODEL_ID_BIN_HEADER_BYTES = Metadata.Key.of("mm-model-id-bin",
            Metadata.BINARY_BYTE_MARSHALLER);

    // should be set on requests from properly load-balanced sources
    static final Metadata.Key<String> BALANCED_META_KEY = Metadata.Key.of("mm-balanced", ASCII_STRING_MARSHALLER);
    // for multi-(v)model requests, can be set to "all" or "any" (default is any)
    static final Metadata.Key<String> REQUIRED_KEY = Metadata.Key.of("mm-required", ASCII_STRING_MARSHALLER);

    // just for backwards compatibility
    static final Metadata.Key<String> MODEL_ID_HEADER_KEY_OLD = Metadata.Key.of("tas-model-id",
            ASCII_STRING_MARSHALLER);

    static Metadata emptyMeta() {
        return new Metadata();
    }

    private static final FastThreadLocal<byte[]> COPY_BUF = new FastThreadLocal<>() {
        @Override
        protected byte[] initialValue() {
            return new byte[32768]; // 32KiB
        }
    };

    static StatusException asLeanException(Status status) {
        StatusException se = new StatusException(status);
        // trim to just the calling method
        se.setStackTrace(new StackTraceElement[] { se.getStackTrace()[1] });
        return se;
    }

    static String toShortString(Status status) {
        String msg = Strings.emptyToNull(status.getDescription());
        if (msg == null) {
            if (status.getCause() != null) {
                msg = status.getCause().toString();
            } else {
                return status.getCode().toString();
            }
        }
        return status.getCode().toString() + ": " + msg;
    }

    /**
     * @see Metadata.AsciiMarshaller
     */
    static boolean isVisibleAscii(String id) {
        for (int i = 0, l = id.length(); i < l; i++) {
            char c = id.charAt(i);
            if (c < 32 || c > 126) {
                return false;
            }
        }
        return true;
    }

    static CharSequence cancellationReason(Context context) {
        Deadline dl = context.getDeadline();
        StringBuilder message = new StringBuilder(64);
        if (dl == null) {
            message.append("cancellation");
        } else {
            long msRemaining = dl.timeRemaining(TimeUnit.MILLISECONDS);
            if (msRemaining <= 0) {
                message.append("deadline expiration");
            } else {
                message.append("cancellation, with ").append(msRemaining).append("ms until deadline");
            }
        }
        Throwable cause = context.cancellationCause();
        if (cause != null && !(cause instanceof TimeoutException)) {
            message.append(" (cause = ").append(cause).append(")");
        }
        return message;
    }

    static final class InterruptingListener implements CancellationListener, AutoCloseable {
        private Thread thread;

        private InterruptingListener() {
            this.thread = Thread.currentThread();
        }

        @Override
        public synchronized void cancelled(Context context) {
            Thread t = thread;
            if (t != null) {
                logger.info("Interrupting request thread " + t.getName()
                        + " due to " + cancellationReason(context));
                t.interrupt();
            }
        }

        @Override
        public void close() {
            synchronized (this) {
                thread = null;
            }
            Thread.interrupted(); // clear flag
        }

        public static InterruptingListener forCurrent() {
            InterruptingListener listener = new InterruptingListener();
            Context.current().addListener(listener, Runnable::run);
            return listener;
        }
    }

    static InterruptingListener newInterruptingListener() {
        return InterruptingListener.forCurrent();
    }

    static boolean warnedOutbound;

    static final class GrpcByteBufInputStream extends ByteBufInputStream implements KnownLength, Drainable {
        protected final ByteBuf buffer; // superclass private unfortunately
        private boolean releaseOnClose = true;

        public GrpcByteBufInputStream(ByteBuf value) {
            super(value, false);
            buffer = value;
        }

        @Override
        public int drainTo(OutputStream target) throws IOException {
            if (!releaseOnClose) {
                return 0;
            }
            if (buffer.readableBytes() >= MIN_ZERO_COPY_SIZE) {
                if (OSA_CLASS != null && OSA_CLASS.isInstance(target)) {
                    try {
                        // If possible, wrap the ByteBuf in a grpc NettyWritableBuffer
                        // and place it directly into the MessageFramer, transferring ownership
                        MessageFramer mf = (MessageFramer) MF_VH.get(target);
                        WritableBuffer wb = (WritableBuffer) MF_BUFFER_VH.get(mf);
                        if (wb != null) {
                            // If there's already a buffer it must be released or committed first
                            if (wb.readableBytes() == 0) {
                                wb.release();
                            } else {
                                MF_CTS_MH.invokeExact(mf, false, false); // commitToSink
                            }
                        }
                        int size = buffer.readableBytes();
                        MF_BUFFER_VH.set(mf, NWB_CTOR_MH.invoke(buffer.touch()));
                        releaseOnClose = false; // ownership transferred
                        // this inputstream must not be used after this point.
                        // we would ideally set the buffer fields to null but
                        // parent class' is private/final
                        return size;
                    } catch (Exception e) {
                        logger.warn("Unexpected reflection error transferring write buffer", e);
                    } catch (Throwable t) {
                        Throwables.throwIfUnchecked(t);
                        throw new RuntimeException(t);
                    }
                }
                if (!warnedOutbound) {
                    logger.warn("Not using using copy-optimized outbound grpc buffer path");
                    warnedOutbound = true;
                }
            }
            if (hasArray(buffer)) {
                int rb = buffer.readableBytes();
                buffer.readBytes(target, rb);
                return rb;
            }
            // Use a larger temp buffer size than netty uses (32KiB instead of 8KiB)
            final byte[] buf = COPY_BUF.get();
            int total = 0, r;
            while ((r = read(buf)) != -1) {
                target.write(buf, 0, r);
                total += r;
            }
            return total;
        }

        @Override
        public void close() throws IOException {
            if (releaseOnClose) {
                releaseOnClose = false;
                buffer.release();
            }
        }
    }

    static boolean hasArray(ByteBuf buffer) {
        if (buffer instanceof CompositeByteBuf) {
            CompositeByteBuf cbb = (CompositeByteBuf) buffer;
            for (int i = 0, n = cbb.numComponents(); i < n; i++) {
                if (!cbb.internalComponent(i).hasArray()) {
                    return false;
                }
            }
            return true;
        }
        return buffer.hasArray();
    }

    /**
     * Bytebufs returned from parse() <b>must</b> be released
     */
    static final Marshaller<ByteBuf> BYTEBUF_MARSHALLER = new Marshaller<>() {
        boolean warnedInbound; // non-volatile ok

        @Override
        public InputStream stream(ByteBuf value) {
            return new GrpcByteBufInputStream(value.touch());
        }

        /**
         * Returned ByteBuf <b>must</b> be released
         */
        @Override
        public ByteBuf parse(InputStream stream) {
            try {
                final int len = stream.available();
                if (len >= MIN_ZERO_COPY_SIZE) {
                    if (RB_BUFFER_MH != null && BIS_CLASS.isInstance(stream)) {
                        try {
                            ReadableBuffer rb = (ReadableBuffer) BIS_BUFFER_VH.get(stream);
                            if (NRB_CLASS.isInstance(rb)) {
                                return ((ByteBuf) RB_BUFFER_MH.invokeExact(rb)).retainedSlice();
                            } else if (rb instanceof CompositeReadableBuffer) {
                                Queue<ReadableBuffer> buffers = (Queue<ReadableBuffer>) CBS_BUFFERS_VH.get(rb);
                                if (buffers.isEmpty()) {
                                    return Unpooled.EMPTY_BUFFER;
                                }
                                if (buffers.size() == 1) {
                                    rb = buffers.peek();
                                    if (NRB_CLASS.isInstance(rb)) {
                                        ByteBuf buf = (ByteBuf) RB_BUFFER_MH.invokeExact(rb);
                                        buffers.remove();
                                        return buf.slice(); // no need to retain since we have removed it
                                    }
                                    // else fall-through to copy fallback
                                } else {
                                    final CompositeByteBuf comp = PooledByteBufAllocator.DEFAULT
                                            .compositeBuffer(buffers.size());
                                    try {
                                        while (!buffers.isEmpty()) {
                                            rb = buffers.peek();
                                            ByteBuf bb;
                                            if (NRB_CLASS.isInstance(rb)) {
                                                bb = (ByteBuf) RB_BUFFER_MH.invokeExact(rb);
                                            } else if (rb.hasArray()) {
                                                bb = Unpooled.wrappedBuffer(rb.array(), rb.arrayOffset(),
                                                        rb.readableBytes());
                                            } else {
                                                int size = rb.readableBytes();
                                                bb = PooledByteBufAllocator.DEFAULT.ioBuffer(
                                                        size); //TODO buffer type tbd
                                                try {
                                                    rb.readBytes(new ByteBufOutputStream(bb), size);
                                                    rb = null;
                                                } catch (IOException ioe) {
                                                    throw new RuntimeException("error reading message stream", ioe);
                                                } finally {
                                                    if (rb != null) {
                                                        bb.release();
                                                    }
                                                }
                                            }
                                            comp.addComponent(true, bb);
                                            buffers.remove();
                                        }
                                        stream = null;
                                        return comp; // no need to retain since we removed them from CRB
                                    } finally {
                                        if (stream != null) {
                                            comp.release();
                                        }
                                    }
                                }
                            }
                        } catch (Exception e) {
                            logger.warn("Unexpected reflection error extracting ByteBuf from stream", e);
                        } catch (Throwable t) {
                            Throwables.throwIfUnchecked(t);
                            throw new RuntimeException(t);
                        }
                    }
                    if (!warnedInbound) {
                        logger.warn("Not using using copy-optimized inbound grpc buffer path");
                        warnedInbound = true;
                    }
                }
                final ByteBuf buf = PooledByteBufAllocator.DEFAULT.ioBuffer(len); //TODO buffer type tbd;
                try {
                    buf.writeBytes(stream, len);
                    stream = null;
                    return buf;
                } finally {
                    if (stream != null) {
                        buf.release();
                    }
                }
            } catch (IOException ioe) {
                throw new RuntimeException("error reading message stream", ioe);
            }
        }
    };

    // ----- method descriptor cache  //TODO prob move these into a separate singleton

    // copy-on-write map
    private static final AtomicReference<Map<String, MethodDescriptor<ByteBuf, ByteBuf>>> descriptors
            = new AtomicReference<>(Collections.emptyMap());

    static MethodDescriptor<ByteBuf, ByteBuf> makeMethodDescriptor(String fullMethodName) {
        return MethodDescriptor.newBuilder(BYTEBUF_MARSHALLER, BYTEBUF_MARSHALLER).setFullMethodName(fullMethodName)
                .setIdempotent(true)
//      .setSafe(true) // causes problems currently

                //TODO only unary methods supported currently;
                // change below to UNKNOWN once streaming is supported, but also check
                // for performance implications to unary case of doing that
                .setType(MethodType.UNARY).build();
    }

    static MethodDescriptor<ByteBuf, ByteBuf> getMethodDescriptorIfPresent(String fullMethodName) {
        return descriptors.get().get(fullMethodName);
    }

    static void addMethodDescriptor(MethodDescriptor<ByteBuf, ByteBuf> descriptor) {
        while (true) {
            Map<String, MethodDescriptor<ByteBuf, ByteBuf>> current = descriptors.get();
            if (current.containsKey(descriptor.getFullMethodName())) {
                return;
            }
            Map<String, MethodDescriptor<ByteBuf, ByteBuf>> copy = new TreeMap<>(current);
            copy.put(descriptor.getFullMethodName(), descriptor);
            if (descriptors.compareAndSet(current, copy)) {
                return;
            }
        }
    }

    static MethodDescriptor<ByteBuf, ByteBuf> getMethodDescriptor(String fullMethodName) {
        MethodDescriptor<ByteBuf, ByteBuf> newMd = null;
        while (true) {
            Map<String, MethodDescriptor<ByteBuf, ByteBuf>> current = descriptors.get();
            MethodDescriptor<ByteBuf, ByteBuf> md = current.get(fullMethodName);
            if (md != null) {
                return md;
            }
            if (newMd == null) {
                newMd = makeMethodDescriptor(fullMethodName);
            }
            Map<String, MethodDescriptor<ByteBuf, ByteBuf>> copy = new TreeMap<>(current);
            copy.put(fullMethodName, newMd);
            if (descriptors.compareAndSet(current, copy)) {
                return newMd;
            }
        }
    }

    static void evictMethodDescriptor(String fullMethodName) {
        while (true) {
            Map<String, MethodDescriptor<ByteBuf, ByteBuf>> current = descriptors.get();
            if (!current.containsKey(fullMethodName)) {
                return;
            }
            Map<String, MethodDescriptor<ByteBuf, ByteBuf>> copy = new TreeMap<>(current);
            copy.remove(fullMethodName);
            if (descriptors.compareAndSet(current, copy)) {
                return;
            }
        }
    }

    private static final byte[] META_GRPC_PFX = "grpc-".getBytes(US_ASCII);
    private static final byte[] META_MM_PFX = "mm-".getBytes(US_ASCII);

    private static final byte[] META_CONT_TYPE = GrpcUtil.CONTENT_TYPE_KEY.name().getBytes(US_ASCII);
    private static final byte[] META_USER_AGENT = GrpcUtil.USER_AGENT_KEY.name().getBytes(US_ASCII);
    private static final byte[] MM_MID_OLD_BYTES = MODEL_ID_HEADER_KEY_OLD.name().getBytes(US_ASCII);

    // filter
    //  - "mm-*", "tas-model-id"
    //  - "grpc-*", "content-type" and "user-agent"
    static boolean ignoreHeader(byte[] arr) {
        if (arr.length < 3) {
            return false;
        }
        return equals(arr, META_CONT_TYPE) || equals(arr, META_USER_AGENT)
               || equals(arr, MM_MID_OLD_BYTES)
               || startsWith(arr, META_GRPC_PFX)
               || startsWith(arr, META_MM_PFX);
    }

    static boolean equals(byte[] arr1, byte[] arr2) {
        return PlatformDependent.equals(arr1, 0, arr2, 0, arr1.length); // faster than Arrays.equals()
    }

    static boolean startsWith(byte[] arr, byte[] prefix) {
        int l = prefix.length;
        return arr.length >= l && PlatformDependent.equals(arr, 0, prefix, 0, l);
    }

    static ByteBuf serializeMetadata(Metadata meta, boolean pooled) {
        int totCount = meta == null ? 0 : InternalMetadata.headerCount(meta) * 2;
        if (totCount == 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        ByteBuf bb = null;
        int countIdx = -1, count = 0;
        byte[][] data = getInternalArray(meta);
        for (int i = 0; i < totCount; i++) {
            byte[] arr = data[i];
            if (i % 2 == 0 && ignoreHeader(arr)) { // key
                i++;
                continue;
            }
            if (bb == null) {
                bb = !pooled ? Unpooled.buffer() : PooledByteBufAllocator.DEFAULT.ioBuffer();
                countIdx = bb.readerIndex();
                bb.writeShort(0); // will overwrite with real count
            }
            bb.writeShort(arr.length).writeBytes(arr);
            count++;
        }
        return bb != null ? bb.setShort(countIdx, count) : Unpooled.EMPTY_BUFFER;
    }

    private static byte[][] getInternalArray(Metadata meta) {
        byte[][] data = null;
        if (namesAndValues != null) {
            try {
                Object nav = namesAndValues.get(meta);
                if (nav instanceof byte[][]) {
                    data = (byte[][]) nav;
                }
            } catch (IllegalAccessException iae) {}
        }
        // fall back to less efficient if can't get via reflection
        return data != null ? data : InternalMetadata.serialize(meta);
    }

    static void skipMetadata(ByteBuffer input) {
        deserializeMetadata0(input, null, false);
    }

    static Metadata deserializeMetadata(ByteBuffer input, String modelId, boolean ascii) {
        int pos = input.position();
        try {
            return deserializeMetadata0(input, modelId, ascii);
        } finally {
            input.position(pos);
        }
    }

    private static Metadata deserializeMetadata0(ByteBuffer input, String modelId, boolean ascii) {
        int metaCount = input != null && input.hasRemaining() ? input.getShort() : 0;
        int size = modelId != null ? metaCount + 4 : metaCount;
        if (size == 0) {
            return emptyMeta();
        }
        byte[][] data = new byte[size][];
        for (int i = 0; i < metaCount; i++) {
            int len = input.getShort();
            input.get(data[i] = new byte[len]);
        }
        Metadata headers = InternalMetadata.newMetadata(metaCount / 2, data);
        return addModelIdHeaders(headers, modelId, ascii);
    }

    private static Metadata addModelIdHeaders(Metadata headers, String modelId, boolean ascii) {
        if (modelId != null) {
            headers.put(MODEL_ID_BIN_HEADER_KEY, modelId);
            if (ascii) {
                headers.put(MODEL_ID_HEADER_KEY, modelId);
            }
        }
        return headers;
    }

    /**
     * Strip ignored headers and add model-id headers
     */
    static Metadata replaceHeaders(Metadata meta, String modelId, boolean ascii) {
        int size = meta == null ? 0 : InternalMetadata.headerCount(meta), j = 0;
        byte[][] origData = getInternalArray(meta);
        byte[][] data = new byte[(size + (ascii ? 1 : 0)) * 2][];
        for (int i = 0; i < size; i++) {
            if (!ignoreHeader(origData[i * 2])) {
                System.arraycopy(origData, i * 2, data, 2 * j++, 2);
            }
        }
        Metadata headers = InternalMetadata.newMetadata(j, data);
        return addModelIdHeaders(headers, modelId, ascii);
    }

    private static final ByteBuffer EMPTY_BB = ByteBuffer.wrap(new byte[0]).asReadOnlyBuffer();

    // serialized metadata is always the first ByteBuffer in the list
    static List<ByteBuffer> toBbList(ByteBuf headersBuf, ByteBuf request) {
        ByteBuffer headerBb;
        if (headersBuf == null || headersBuf.readableBytes() <= 2) {
            headerBb = EMPTY_BB;
        } else if (headersBuf.nioBufferCount() == 1) {
            headerBb = headersBuf.nioBuffer();
        } else {
            headerBb = ByteBuffer.allocate(headersBuf.readableBytes());
            headersBuf.getBytes(headersBuf.readerIndex(), headerBb);
            headerBb.flip();
        }
        return request.nioBufferCount() == 1 ? Arrays.asList(headerBb, request.nioBuffer())
                : Lists.asList(headerBb, request.nioBuffers());
    }

    // returned ByteString (and derivatives) *must not* be used after buf is released
    static ByteString unsafeWrappedByteString(ByteBuf buf) {
        if (buf.hasArray()) {
            return UnsafeByteOperations.unsafeWrap(buf.array(), buf.arrayOffset() + buf.readerIndex(),
                    buf.readableBytes());
        }
        int count = buf.nioBufferCount();
        if (count == 1) {
            return UnsafeByteOperations.unsafeWrap(buf.nioBuffer());
        }
        if (count <= 0) {
            return UnsafeByteOperations.unsafeWrap(ByteBufUtil.getBytes(buf));
        }
        ByteString bs = null;
        for (ByteBuffer bb : buf.nioBuffers()) {
            ByteString thisBs = UnsafeByteOperations.unsafeWrap(bb);
            bs = bs == null ? thisBs : bs.concat(thisBs);
        }
        return bs;
    }

    /**
     * ignore first element (metadata)
     */
    static ByteBuf toMessage(List<ByteBuffer> bbs) {
        int numBufs = bbs.size();
        if (numBufs <= 1) {
            return Unpooled.EMPTY_BUFFER;
        }
        ByteBuf buf;
        if (numBufs == 2) {
            buf = wrapWithOwnership(bbs.get(1));
        } else {
            ByteBuf[] components = new ByteBuf[numBufs - 1];
            int j = 0;
            for (int i = 1; i < numBufs; i++) {
                ByteBuffer b = bbs.get(i);
                if (b == null) {
                    break;
                }
                if (b.remaining() != 0) {
                    b.order(ByteOrder.BIG_ENDIAN);
                    components[j] = j == 0 ? wrapWithOwnership(b) : Unpooled.wrappedBuffer(b);
                    j++;
                }
            }
            if (j == 0) {
                return Unpooled.EMPTY_BUFFER;
            }
            if (j < components.length) {
                if (j == 1) {
                    return components[0];
                }
                components = Arrays.copyOf(components, j, ByteBuf[].class);
            }
            buf = Unpooled.wrappedUnmodifiableBuffer(components);
        }
        ReleaseAfterResponse.addReleasable(buf);
        return buf;
    }

    // Like Unpooled.wrapBuffer(ByteBuffer) but binds ownership of any litelinks request-scoped
    // resources to the resulting ByteBuf
    static ByteBuf wrapWithOwnership(ByteBuffer buffer) {
        AutoCloseable toRelease = ReleaseAfterResponse.takeOwnership();
        if (toRelease == null) {
            return Unpooled.wrappedBuffer(buffer);
        }
        if (!buffer.isDirect() && buffer.hasArray()) {
            ByteBuf buf = new OwnerUnpooledHeapByteBuf(buffer.array(), toRelease).order(buffer.order());
            int off = buffer.arrayOffset() + buffer.position();
            return off != 0 || buffer.remaining() != buffer.array().length
                    ? buf.slice(off, buffer.remaining()) : buf;
        } else if (PlatformDependent.hasUnsafe()) {
            return new OwnerUnpooledUnsafeDirectByteBuf(buffer, toRelease);
        } else {
            return new OwnerUnpooledDirectByteBuf(buffer, toRelease);
        }
    }

    static final class OwnerUnpooledUnsafeDirectByteBuf extends UnpooledUnsafeDirectByteBuf {
        private final AutoCloseable toClose;

        public OwnerUnpooledUnsafeDirectByteBuf(ByteBuffer buffer, AutoCloseable owned) {
            super(UnpooledByteBufAllocator.DEFAULT, buffer, buffer.remaining());
            this.toClose = owned;
        }

        @Override
        protected void deallocate() {
            super.deallocate();
            closeQuietly(toClose);
        }
    }

    static final class OwnerUnpooledDirectByteBuf extends UnpooledDirectByteBuf {
        private final AutoCloseable toClose;

        public OwnerUnpooledDirectByteBuf(ByteBuffer buffer, AutoCloseable owned) {
            super(UnpooledByteBufAllocator.DEFAULT, buffer, buffer.remaining());
            this.toClose = owned;
        }

        @Override
        protected void deallocate() {
            super.deallocate();
            closeQuietly(toClose);
        }
    }

    static final class OwnerUnpooledHeapByteBuf extends UnpooledHeapByteBuf {
        private final AutoCloseable toClose;

        public OwnerUnpooledHeapByteBuf(byte[] arr, AutoCloseable owned) {
            super(UnpooledByteBufAllocator.DEFAULT, arr, arr.length);
            this.toClose = owned;
        }

        @Override
        protected void deallocate() {
            super.deallocate();
            closeQuietly(toClose);
        }
    }

    static void closeQuietly(AutoCloseable c) {
        try {
            c.close();
        } catch (Exception e) {
            logger.warn("Unexpected error releasing resources", e);
        }
    }

    private static final Field namesAndValues;

    static {
        Field nav = null;
        try {
            nav = Metadata.class.getDeclaredField("namesAndValues");
            nav.setAccessible(true);
        } catch (Exception e) {
        }
        namesAndValues = nav;
    }

    // for grpc/netty internal ReadableBuffer reflection to minimize data copies (inbound)
    private static final Class<? extends InputStream> BIS_CLASS; // ReadableBuffers$BufferInputSream
    private static final Class<? extends ReadableBuffer> NRB_CLASS; // NettyReadableBuffer
    private static final VarHandle BIS_BUFFER_VH; // BufferInputSream#buffer
    private static final VarHandle CBS_BUFFERS_VH; // CompositeReadableBuffer#buffers
    private static final MethodHandle RB_BUFFER_MH; // NettyReadableBuffer#buffer()

    static {
        Class<? extends InputStream> bc;
        Class<? extends ReadableBuffer> nc;
        MethodHandle bmh;
        VarHandle rbvh, vbvh;
        try {
            Lookup lookup = MethodHandles.lookup();
            bc = Class.forName("io.grpc.internal.ReadableBuffers$BufferInputStream")
                    .asSubclass(InputStream.class);
            nc = Class.forName("io.grpc.netty.NettyReadableBuffer")
                    .asSubclass(ReadableBuffer.class);
            Class<?> crbc = CompositeReadableBuffer.class;
            Field cf;
            try {
                cf = crbc.getDeclaredField("readableBuffers"); // grpc >= 1.39.0
            } catch (NoSuchFieldException nsfe) {
                cf = crbc.getDeclaredField("buffers"); // grpc < 1.39.0
            }
            cf.setAccessible(true);
            vbvh = MethodHandles.privateLookupIn(crbc, lookup).unreflectVarHandle(cf);
            Field rf = bc.getDeclaredField("buffer");
            rf.setAccessible(true);
            rbvh = MethodHandles.privateLookupIn(bc, lookup).unreflectVarHandle(rf);
            Method bm = nc.getDeclaredMethod("buffer");
            bm.setAccessible(true);
            bmh = lookup.unreflect(bm);
            bmh = bmh.asType(java.lang.invoke.MethodType.methodType(ByteBuf.class, ReadableBuffer.class));
        } catch (Exception e) {
            logger.warn("Unable to set up reflection for inbound gRPC IO buffer optimization", e);
            bc = null;
            nc = null;
            rbvh = vbvh = null;
            bmh = null;
        }
        BIS_CLASS = bc;
        NRB_CLASS = nc;
        BIS_BUFFER_VH = rbvh;
        CBS_BUFFERS_VH = vbvh;
        RB_BUFFER_MH = bmh;
    }

    // for grpc/netty internal WritableBuffer reflection to minimize data copies (outbound)
    private static final Class<? extends OutputStream> OSA_CLASS; // MessageFramer$OutpuStreamAdapter
    private static final VarHandle MF_VH, MF_BUFFER_VH; // outer MessageFramer instance, MessageFramer#buffer
    private static final MethodHandle NWB_CTOR_MH; // NettyWritableBuffer constructor
    private static final MethodHandle MF_CTS_MH; // MessageFramer#commitToSink()

    static {
        Class<? extends OutputStream> osac;
        VarHandle mfvh, mbfvh;
        MethodHandle nwbch, ctsmh;
        try {
            Lookup lookup = MethodHandles.lookup();
            Class<?> mfc = MessageFramer.class;
            Optional<Class<?>> maybeOsac = Arrays.stream(mfc.getDeclaredClasses())
                    .filter(c -> "OutputStreamAdapter".equals(c.getSimpleName())).findFirst();
            osac = maybeOsac.orElseThrow(() -> new ClassNotFoundException(
                    "io.grpc.internal.MessageFramer$OutputStreamAdapter")).asSubclass(OutputStream.class);
            Field mf = osac.getDeclaredField("this$0");
            mf.setAccessible(true);
            mfvh = MethodHandles.privateLookupIn(osac, lookup).unreflectVarHandle(mf);
            Method ctsm = mfc.getDeclaredMethod("commitToSink", Boolean.TYPE, Boolean.TYPE);
            ctsm.setAccessible(true);
            ctsmh = lookup.unreflect(ctsm);
            Field mfbf = mfc.getDeclaredField("buffer");
            mfbf.setAccessible(true);
            mbfvh = MethodHandles.privateLookupIn(mfc, lookup).unreflectVarHandle(mfbf);
            Class<? extends WritableBuffer> nwbClz = Class.forName("io.grpc.netty.NettyWritableBuffer")
                    .asSubclass(WritableBuffer.class);
            Constructor<? extends WritableBuffer> nwbc = nwbClz.getDeclaredConstructor(ByteBuf.class);
            nwbc.setAccessible(true);
            nwbch = lookup.unreflectConstructor(nwbc);
        } catch (Exception e) {
            logger.warn("Unable to set up reflection for outbound gRPC IO buffer optimization", e);
            osac = null;
            mfvh = mbfvh = null;
            ctsmh = nwbch = null;
        }
        OSA_CLASS = osac;
        MF_VH = mfvh;
        MF_CTS_MH = ctsmh;
        MF_BUFFER_VH = mbfvh;
        NWB_CTOR_MH = nwbch;
    }
}
