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
 */
package com.ibm.watson.statsd;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.timgroup.statsd.StatsDClientErrorHandler;

public class StatsDSender implements Runnable {
    private static final Charset MESSAGE_CHARSET = Charset.forName("UTF-8");

    private final CharsetEncoder utf8Encoder = MESSAGE_CHARSET.newEncoder()
            .onMalformedInput(CodingErrorAction.REPLACE)
            .onUnmappableCharacter(CodingErrorAction.REPLACE);

    private final StringBuilder builder = new StringBuilder();
    private CharBuffer charBuffer = CharBuffer.wrap(builder);

    private final ByteBuffer sendBuffer;
    private final Callable<SocketAddress> addressLookup;
    private final BlockingQueue<Message> queue;
    private final StatsDClientErrorHandler handler;
    private final DatagramChannel clientChannel;

    private final boolean eopNewline;

    private volatile boolean shutdown;


    public StatsDSender(final Callable<SocketAddress> addressLookup, final int queueSize,
                 final StatsDClientErrorHandler handler, final DatagramChannel clientChannel, final int maxPacketSizeBytes) {
        this(addressLookup, new LinkedBlockingQueue<Message>(queueSize), handler, clientChannel,
                maxPacketSizeBytes, false);
    }

    public StatsDSender(final Callable<SocketAddress> addressLookup, final BlockingQueue<Message> queue,
                 final StatsDClientErrorHandler handler, final DatagramChannel clientChannel,
                 final int maxPacketSizeBytes, final boolean eopNewline) {
        sendBuffer = ByteBuffer.allocate(maxPacketSizeBytes);
        this.addressLookup = addressLookup;
        this.queue = queue;
        this.handler = handler;
        this.clientChannel = clientChannel;
        this.eopNewline = eopNewline;
    }

    interface Message {
        /**
         * Write this message to the provided {@link StringBuilder}. Will
         * only ever be called from the sender thread.
         *
         * @param builder
         */
        void writeTo(StringBuilder builder);
    }

    boolean send(final Message message) {
        if (!shutdown) {
            queue.offer(message);
            return true;
        }
        return false;
    }

    private static final Callable<SocketAddress> NULL = new Callable<SocketAddress>() {
        @Override public SocketAddress call() {
            return null;
        }
    };

    @Override
    public void run() {
        Callable<SocketAddress> addressLookup = clientChannel.isConnected() ? NULL : this.addressLookup;
        while (!(queue.isEmpty() && shutdown)) {
            try {
                if (Thread.interrupted()) {
                    return;
                }
                final Message message = queue.poll(1, TimeUnit.SECONDS);
                if (null != message) {
                    final SocketAddress address = addressLookup.call();
                    builder.setLength(0);
                    message.writeTo(builder);
                    int lowerBoundSize = builder.length();
                    if (sendBuffer.remaining() < (lowerBoundSize + 1)) {
                        blockingSend(address);
                    }
                    if (eopNewline) {
                        builder.append('\n');
                    } else {
                        sendBuffer.mark();
                        if (sendBuffer.position() > 0) {
                            sendBuffer.put((byte) '\n');
                        }
                    }
                    if (!writeBuilderToSendBuffer()) {
                        sendBuffer.reset();
                        blockingSend(address);
                        if (!writeBuilderToSendBuffer()) {
                            //TODO TBD what to do with buffer
                            throw new BufferOverflowException();
                        }
                    }
                    if (null == queue.peek()) {
                        blockingSend(address);
                    }
                }
            } catch (final InterruptedException e) {
                if (shutdown) {
                    return;
                }
            } catch (final Exception e) {
                handler.handle(e);
            }
        }
        builder.setLength(0);
        builder.trimToSize();
    }

    private boolean writeBuilderToSendBuffer() {
        int length = builder.length();
        // use existing charbuffer if possible, otherwise re-wrap
        if (length <= charBuffer.capacity()) {
            charBuffer.limit(length).position(0);
        } else {
            charBuffer = CharBuffer.wrap(builder);
        }
        return utf8Encoder.encode(charBuffer, sendBuffer, true) != CoderResult.OVERFLOW;
    }

    private void blockingSend(final SocketAddress address) throws IOException {
        final int sizeOfBuffer = sendBuffer.position();
        sendBuffer.flip();

        final int sentBytes = address != null ? clientChannel.send(sendBuffer, address)
                : clientChannel.write(sendBuffer);
        sendBuffer.limit(sendBuffer.capacity());
        sendBuffer.rewind();

        if (sizeOfBuffer != sentBytes) {
            handler.handle(
                    new IOException(
                            String.format(
                                    "Could not send entirely stat %s to %s. Only sent %d bytes out of %d bytes",
                                    sendBuffer.toString(),
                                    address.toString(),
                                    sentBytes,
                                    sizeOfBuffer)));
        }
    }

    boolean isShutdown() {
        return shutdown;
    }

    void shutdown() {
        shutdown = true;
    }
}
