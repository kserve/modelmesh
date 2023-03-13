/*
 * Copyright 2023 IBM Corporation
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

package com.ibm.watson.modelmesh.payload;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An async {@link PayloadProcessor} that queues processing requests and process them asynchronously.
 */
public class AsyncPayloadProcessor implements PayloadProcessor {

    private static final Logger logger = LoggerFactory.getLogger(AsyncPayloadProcessor.class);

    private final PayloadProcessor delegate;

    private final LinkedBlockingDeque<Payload> payloads;

    private final AtomicInteger dropped;

    private final ScheduledExecutorService executorService;

    public AsyncPayloadProcessor(PayloadProcessor delegate) {
        this(delegate, 1, TimeUnit.MINUTES, Executors.newScheduledThreadPool(2), 64);
    }

    public AsyncPayloadProcessor(PayloadProcessor delegate, int delay, TimeUnit timeUnit,
                                 ScheduledExecutorService executorService, int capacity) {
        this.delegate = delegate;
        this.dropped = new AtomicInteger();
        this.payloads = new LinkedBlockingDeque<>(capacity);
        this.executorService = executorService;

        this.executorService.execute(() -> {
            try {
                while (true) {
                    processPayload(payloads.take());
                }
            } catch (InterruptedException ie) {
                // Here we assume that we're shutting down
                logger.info("Payload queue processing interrupted");
            }
            // Process any remaining payloads in the queue
            for (Payload p; (p = payloads.poll()) != null;) {
                processPayload(p);
            }
            try {
                this.delegate.close();
            } catch (IOException e) {
                // ignore
            }
            logger.info("AsyncPayloadProcessor task exiting");
        });

        this.executorService.scheduleWithFixedDelay(() -> {
            if (dropped.get() > 0) {
                int droppedRequest = dropped.getAndSet(0);
                logger.warn("{} payloads were dropped because of {} capacity limit in the last {} {}", droppedRequest,
                            capacity, delay, timeUnit);
            }
        }, 0, delay, timeUnit);
    }

    void processPayload(Payload p) {
        boolean released = false;
        try {
            released = delegate.process(p);
        } catch (Throwable t) {
            logger.warn("Error while processing payload: {}", p, t);
        } finally {
            if (!released) {
                p.release();
            }
        }
    }

    @Override
    public boolean mayTakeOwnership() {
        return true;
    }

    @Override
    public String getName() {
        return delegate.getName() + "-async";
    }

    @Override
    public boolean process(Payload payload) {
        boolean enqueued = payloads.offer(payload);
        if (!enqueued) {
            dropped.incrementAndGet();
        }
        return enqueued;
    }

    @Override
    public void close() throws IOException {
        this.executorService.shutdownNow();
    }

}
