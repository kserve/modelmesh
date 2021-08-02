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

import io.grpc.KnownLength;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream implements KnownLength {
    protected final ByteBuffer bb;

    public ByteBufferInputStream(ByteBuffer bb) {
        this.bb = bb;
    }

    @Override
    public int read() {
        return bb.remaining() > 0 ? bb.get() & 0xff : -1;
    }

    @Override
    public int available() {
        return bb.remaining();
    }

    @Override
    public int read(byte[] b, int off, int len) {
        int rem = bb.remaining();
        if (rem == 0) {
            return -1;
        }
        if (rem < len) {
            len = rem;
        }
        bb.get(b, off, len);
        return len;
    }

    @Override
    public long skip(long n) {
        int skipBytes = Math.min(bb.remaining(), Math.toIntExact(n));
        if (skipBytes > 0) {
            bb.position(bb.position() + skipBytes);
        }
        return skipBytes;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readlimit) {
        bb.mark();
    }

    @Override
    public void reset() {
        bb.reset();
    }
}
