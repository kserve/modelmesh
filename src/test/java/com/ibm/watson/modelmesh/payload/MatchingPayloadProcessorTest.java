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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MatchingPayloadProcessorTest {

    @Test
    void testPayloadProcessingAny() {
        AtomicInteger requestCount = new AtomicInteger();
        AtomicInteger responseCount = new AtomicInteger();
        PayloadProcessor delegate = new DummyPayloadProcessor(requestCount, responseCount);
        MatchingPayloadProcessor payloadProcessor = new MatchingPayloadProcessor(delegate, null, null);
        payloadProcessor.processRequest(new Payload(null, null, null, null, null));
        assertEquals(1, requestCount.get());
        payloadProcessor.processRequest(new Payload("someModelId", null, null, null, null));
        assertEquals(2, requestCount.get());
        payloadProcessor.processRequest(new Payload(null, null, "processRequest", null, null));
        assertEquals(3, requestCount.get());
        payloadProcessor.processRequest(new Payload("someModelId", null, "processRequest", null, null));
        assertEquals(4, requestCount.get());
    }

    @Test
    void testPayloadProcessingModelFilter() {
        AtomicInteger requestCount = new AtomicInteger();
        AtomicInteger responseCount = new AtomicInteger();
        PayloadProcessor delegate = new DummyPayloadProcessor(requestCount, responseCount);
        MatchingPayloadProcessor payloadProcessor = new MatchingPayloadProcessor(delegate, null, "someModelId");
        payloadProcessor.processRequest(new Payload(null, null, null, null, null));
        assertEquals(0, requestCount.get());
        payloadProcessor.processRequest(new Payload("someModelId", null, null, null, null));
        assertEquals(1, requestCount.get());
        payloadProcessor.processRequest(new Payload(null, null, "processRequest", null, null));
        assertEquals(1, requestCount.get());
        payloadProcessor.processRequest(new Payload("someModelId", null, "processRequest", null, null));
        assertEquals(2, requestCount.get());
    }

    @Test
    void testPayloadProcessingMethodFilter() {
        AtomicInteger requestCount = new AtomicInteger();
        AtomicInteger responseCount = new AtomicInteger();
        PayloadProcessor delegate = new DummyPayloadProcessor(requestCount, responseCount);
        MatchingPayloadProcessor payloadProcessor = new MatchingPayloadProcessor(delegate, "getName", null);
        payloadProcessor.processRequest(new Payload(null, null, null, null, null));
        assertEquals(0, requestCount.get());
        payloadProcessor.processRequest(new Payload("someModelId", null, null, null, null));
        assertEquals(0, requestCount.get());
        payloadProcessor.processRequest(new Payload(null, null, "getName", null, null));
        assertEquals(1, requestCount.get());
        payloadProcessor.processRequest(new Payload("someModelId", null, "getName", null, null));
        assertEquals(2, requestCount.get());
    }
}