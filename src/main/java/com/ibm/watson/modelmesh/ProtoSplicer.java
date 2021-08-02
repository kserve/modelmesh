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

import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import static com.google.protobuf.WireFormat.*;

/**
 * Splice encoded protobuf messages in {@link ByteBuf}s.
 */
public final class ProtoSplicer {

    private ProtoSplicer() {} // static only

    /**
     * Reads a single string protobuf value from a field at the specified path within a
     * {@link ByteBuf} containing an encoded protobuf message.
     *
     * @return extracted string, or {@code null} if not found
     */
    public static String extractId(ByteBuf buf, int[] fieldPath) {

        // throws if fieldPath is invalid
        final int containerCount = containerCount(fieldPath);
        final int startIndex = buf.readerIndex();
        try {
            for (int curEndIndex = buf.writerIndex(), curPathIdx = 0; ; ) {
                final int fieldStartIndex = buf.readerIndex();
                if (fieldStartIndex >= curEndIndex) {
                    if (fieldStartIndex > curEndIndex) {
                        throw new RuntimeException("proto parsing error");
                    }
                    return null; // field not found, maybe throw, TBD
                }
                final int tag = readVI32(buf), type = getTagWireType(tag);
                // skip over fields that we are not interested in
                if (getTagFieldNumber(tag) != fieldPath[curPathIdx]) {
                    skipField(type, buf);
                    continue;
                }
                if (type != WIRETYPE_LENGTH_DELIMITED) {
                    throw new RuntimeException("proto message id field is not a string (type = " + type + ")");
                }
                final int contentLen = readVI32(buf), contentStartIndex = buf.readerIndex();
                if (curPathIdx == containerCount) {
                    // we have found our value
                    return buf.toString(contentStartIndex, contentLen, StandardCharsets.UTF_8);
                }
                // this is an embedded message field, descend within it
                curEndIndex = contentStartIndex + contentLen;
                curPathIdx++;
            }
        } finally {
            buf.readerIndex(startIndex);
        }
    }

    /**
     * Surgically inserts or updates a specific length-delimited protobuf field in a {@link ByteBuf}
     * containing an encoded protobuf message. Splices/references the provided buffer as needed without
     * modifying or copying it, returning a new {@link ByteBuf} unless no modification is needed.
     *
     * @return bytebuf takes over ownership of passed buf (and could be same object), must be released
     */
    public static ByteBuf spliceId(ByteBuf buf, int[] fieldPath, byte[] idUtf8Bytes) {

        // throws if fieldPath is invalid
        final int containerCount = containerCount(fieldPath);

        final int startIndex = buf.readerIndex(), endIndex = buf.writerIndex();
        final int idUtf8Len = idUtf8Bytes.length, idLenLen = vi32len(idUtf8Bytes.length);
        // idBuf is len-prefixed id bytes
        ByteBuf idBuf = PooledByteBufAllocator.DEFAULT.buffer(idLenLen + idUtf8Len), wrappers = null;
        int toRelease = 0;
        try {
            writeVI32(idBuf, idUtf8Len).writeBytes(idUtf8Bytes);

            //TODO maybe combine these, e.g. into a single long[]
            final byte[] msgLenLens = containerCount > 0 ? new byte[containerCount] : null;
            final int[] msgLens = containerCount > 0 ? new int[containerCount] : null;
            final int[] msgLenOffsets = containerCount > 0 ? new int[containerCount] : null;

            int curEndIndex = endIndex, curTargetFieldNum = fieldPath[0], curPathIdx = 0;
            while (true) {
                int lenDelta, upToIndex;
                final int fieldStartIndex = buf.readerIndex();
                if (fieldStartIndex < curEndIndex) {
                    final int tag = readVI32(buf), type = getTagWireType(tag);
                    // skip over fields that we are not interested in
                    if (getTagFieldNumber(tag) != curTargetFieldNum) {
                        skipField(type, buf);
                        continue;
                    }
                    if (type != WIRETYPE_LENGTH_DELIMITED) {
                        throw new RuntimeException("proto message id field is not a string (type = " + type + ")");
                    }
                    final int lenDelimIndex = buf.readerIndex();
                    final int contentLen = readVI32(buf), contentStartIndex = buf.readerIndex();
                    curEndIndex = contentStartIndex + contentLen;
                    if (curPathIdx < containerCount) {
                        // this is an embedded message field, descend within it
                        msgLenOffsets[curPathIdx] = lenDelimIndex;
                        msgLenLens[curPathIdx] = (byte) (contentStartIndex - lenDelimIndex);
                        msgLens[curPathIdx] = contentLen;
                        curTargetFieldNum = fieldPath[++curPathIdx];
                        continue;
                    }
                    // we have reached the target string to update
                    lenDelta = idUtf8Len + idLenLen - (curEndIndex - lenDelimIndex);
                    if (lenDelta == 0) {
                        // return as-is if already has correct contents
                        if (ByteBufUtil.equals(buf, contentStartIndex, idBuf, idLenLen, contentLen)) {
                            return buf;
                        }

                        // just splice the id, don't need to adjust containing message lengths
                        ByteBuf result = wrap(buf.slice(startIndex, lenDelimIndex - startIndex), idBuf,
                                buf.retainedSlice(curEndIndex, endIndex - curEndIndex));
                        idBuf = null;
                        return result;
                    }
                    upToIndex = lenDelimIndex;
                } else if (fieldStartIndex == curEndIndex) {
                    // this is the case where the target field doesn't already exist;
                    // need to pad with tags / len delims in addition to the new field
                    lenDelta = idUtf8Len + idLenLen;
                    upToIndex = curEndIndex;
                    int capacity = 4 + 8 * (containerCount - curPathIdx), leftIndex = capacity;
                    wrappers = PooledByteBufAllocator.DEFAULT.buffer(capacity);
                    final int strTag = makeTag(fieldPath[containerCount]), strTagLen = vi32len(strTag);
                    writeVI32(wrappers.writerIndex(leftIndex -= strTagLen), strTag);
                    lenDelta += strTagLen;
                    for (int i = containerCount - 1; i >= curPathIdx; i--) {
                        int lenDelimLen = vi32len(lenDelta);
                        writeVI32(wrappers.writerIndex(leftIndex -= lenDelimLen), lenDelta);
                        int msgTag = makeTag(fieldPath[i]), msgTagLen = vi32len(msgTag);
                        writeVI32(wrappers.writerIndex(leftIndex -= msgTagLen), msgTag);
                        lenDelta += lenDelimLen + msgTagLen;
                    }
                    wrappers = wrappers.slice(leftIndex, capacity - leftIndex);
                } else {
                    throw new RuntimeException("proto parsing error");
                }

                // here lenDelta != 0
                int bbParts = 2 * curPathIdx + 3;
                if (wrappers != null) {
                    bbParts++;
                }
                final ByteBuf[] parts = new ByteBuf[bbParts];

                // work backwards - need to include all later mods in earlier length adjustments
                parts[bbParts - 1] = buf.slice(curEndIndex, endIndex - curEndIndex);
                parts[bbParts - 2] = idBuf;
                if (wrappers != null) {
                    parts[bbParts - 3] = wrappers;
                }

                for (int i = curPathIdx - 1; i >= 0; i--) {
                    int msgLenOffset = msgLenOffsets[i], lastPartStart = msgLenOffset + msgLenLens[i];
                    parts[2 * i + 2] = buf.retainedSlice(lastPartStart, upToIndex - lastPartStart);
                    toRelease++;
                    upToIndex = msgLenOffset;
                    int newLen = msgLens[i] + lenDelta, newLenLen = vi32len(newLen);
                    parts[2 * i + 1] = encVI32(newLen, newLenLen);
                    lenDelta += (newLenLen - msgLenLens[i]);
                }
                parts[0] = buf.retainedSlice(startIndex, upToIndex - startIndex);
                final ByteBuf result = wrap(parts);
                idBuf = null;
                wrappers = null;
                toRelease = 0;
                return result;
            }
        } finally {
            buf.readerIndex(startIndex);
            if (idBuf != null) idBuf.release();
            if (wrappers != null) wrappers.release();
            if (toRelease > 0) buf.release(toRelease);
        }
    }

    private static int containerCount(int[] fieldPath) {
        if (fieldPath == null || fieldPath.length == 0) {
            throw new IllegalArgumentException("must use non-empty fieldPath");
        }
        return fieldPath.length - 1;
    }

    private static void skipField(int type, ByteBuf buf) {
        if (type == WIRETYPE_VARINT) skipVI(buf);
        else if (type == WIRETYPE_FIXED32) buf.skipBytes(4);
        else if (type == WIRETYPE_FIXED64) buf.skipBytes(8);
        else if (type == WIRETYPE_LENGTH_DELIMITED) buf.skipBytes(readVI32(buf));
        else throw new RuntimeException("unsupported protobuf field type:" + type);
    }

    static int vi32len(int val) {
        int bits = 32 - Integer.numberOfLeadingZeros(val);
        return (bits / 7) + (bits % 7 == 0 ? 0 : 1);
    }

    static ByteBuf writeVI32(ByteBuf buf, int val) {
        while (true) {
            if ((val & ~0x7F) == 0) {
                return buf.writeByte(val);
            }
            buf.writeByte((val & 0x7F) | 0x80);
            val >>>= 7;
        }
    }

    static ByteBuf encVI32(int val) {
        return encVI32(val, vi32len(val));
    }

    static ByteBuf encVI32(int val, int byteLen) {
        ByteBuf buf = Unpooled.buffer(byteLen); //TODO *maybe* pooled
        return writeVI32(buf, val);
    }

    static ByteBuf wrap(ByteBuf... buffers) {
        return Unpooled.wrappedUnmodifiableBuffer(buffers);
    }

    static final int MAX_VARINT_SIZE = 10;

    static void skipVI(ByteBuf bb) {
        for (int i = 0; i < MAX_VARINT_SIZE; i++) {
            if (bb.readByte() >= 0) {
                return;
            }
        }
        throw new RuntimeException(); //TODO
    }

    static int makeTag(int fieldNumber) {
        return (fieldNumber << 3) | WIRETYPE_LENGTH_DELIMITED;
    }

    static int readVI32(ByteBuf bb) {
        try {
            return (int) readVarIntMh.invokeExact(bb);
        } catch (Throwable t) {
            Throwables.throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
    }

    // ---- access private ProtobufVarint32FrameDecoder#readRawVarint32 method efficiently

    private static final MethodHandle readVarIntMh;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Method readVarIntMeth = ProtobufVarint32FrameDecoder.class
                    .getDeclaredMethod("readRawVarint32", ByteBuf.class);
            readVarIntMeth.setAccessible(true);
            readVarIntMh = lookup.unreflect(readVarIntMeth);
        } catch (Exception e) {
            throw new RuntimeException("Unsupported netty version", e);
        }
    }

}
