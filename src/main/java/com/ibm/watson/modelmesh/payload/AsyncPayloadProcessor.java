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

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class AsyncPayloadProcessor implements PayloadProcessor {

    private static final Logger logger = LoggerFactory.getLogger(AsyncPayloadProcessor.class);

    private final PayloadProcessor delegate;

    private final FixedSizeConcurrentLinkedDeque<Payload> requestPayloads;
    private final FixedSizeConcurrentLinkedDeque<Payload> responsePayloads;

    public AsyncPayloadProcessor(PayloadProcessor delegate) {
        this(delegate, 1, TimeUnit.MINUTES, Executors.newScheduledThreadPool(1), 1000);
    }

    public AsyncPayloadProcessor(PayloadProcessor delegate, int delay, TimeUnit timeUnit,
                                 ScheduledExecutorService executorService, int capacity) {
        this.delegate = delegate;
        this.requestPayloads = new FixedSizeConcurrentLinkedDeque<>(capacity);
        this.responsePayloads = new FixedSizeConcurrentLinkedDeque<>(capacity);

        executorService.scheduleWithFixedDelay(() -> {
            Payload p;
            while ((p = requestPayloads.poll()) != null) {
                delegate.processRequest(p);
            }
            int droppedRequest = requestPayloads.dropped.getAndSet(0);
            if (droppedRequest > 0) {
                logger.warn("{} request payloads were skipped because of {} capacity limit", droppedRequest, capacity);
            }
            while ((p = responsePayloads.poll()) != null) {
                delegate.processResponse(p);
            }
            int droppedResponse = responsePayloads.dropped.getAndSet(0);
            if (droppedResponse > 0) {
                logger.warn("{} response payloads were skipped because of {} capacity limit", droppedResponse, capacity);
            }
        }, 0, delay, timeUnit);
    }

    @Override
    public String getName() {
        return delegate.getName() + "-async";
    }

    @Override
    public void processRequest(Payload payload) {
        requestPayloads.offer(payload);
    }

    @Override
    public void processResponse(Payload payload) {
        responsePayloads.offer(payload);
    }

    /**
     * A {@link ConcurrentLinkedDeque} with fixed maximum capacity.
     * When newer elements are offered while being at full capacity, the oldest elements are removed.
     * A counter of items that were dropped from the queue because of exceeding capacity is kept.
     *
     * @param <T>
     */
    static class FixedSizeConcurrentLinkedDeque<T> extends ConcurrentLinkedDeque<T> {

        private final AtomicInteger dropped;

        private final int capacity;

        FixedSizeConcurrentLinkedDeque(int capacity) {
            this.capacity = capacity;
            dropped = new AtomicInteger();
        }

        @Override
        public boolean offer(T o) {
            return super.offer(o) && (size() <= capacity || (super.pop() != null && dropped.incrementAndGet() < capacity));
        }

        public int getDropped() {
            return dropped.get();
        }
    }
}
