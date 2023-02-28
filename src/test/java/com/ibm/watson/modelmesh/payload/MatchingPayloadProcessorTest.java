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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MatchingPayloadProcessorTest {

    @Test
    void testPayloadProcessingAny() {
        AtomicInteger requestCount = new AtomicInteger();
        AtomicInteger responseCount = new AtomicInteger();
        PayloadProcessor delegate = new DummyPayloadProcessor(requestCount, responseCount);
        MatchingPayloadProcessor payloadProcessor = MatchingPayloadProcessor.from(null, null, delegate);
        payloadProcessor.processRequest(new Payload(null, null, null, null, null, null));
        assertEquals(1, requestCount.get());
        payloadProcessor.processRequest(new Payload(null, "someModelId", null, null, null, null));
        assertEquals(2, requestCount.get());
        payloadProcessor.processRequest(new Payload(null, null, null, "processRequest", null, null));
        assertEquals(3, requestCount.get());
        payloadProcessor.processRequest(new Payload(null, "someModelId", null, "processRequest", null, null));
        assertEquals(4, requestCount.get());
    }

    @Test
    void testPayloadProcessingAnySpecialChars() {
        AtomicInteger requestCount = new AtomicInteger();
        AtomicInteger responseCount = new AtomicInteger();
        DummyPayloadProcessor delegate = new DummyPayloadProcessor(requestCount, responseCount);
        List<PayloadProcessor> processors = new ArrayList<>();
        processors.add(MatchingPayloadProcessor.from("", "", delegate));
        processors.add(MatchingPayloadProcessor.from("*", "", delegate));
        processors.add(MatchingPayloadProcessor.from("/*", "", delegate));
        processors.add(MatchingPayloadProcessor.from("/*", "", delegate));
        processors.add(MatchingPayloadProcessor.from("/*", "*", delegate));
        processors.add(MatchingPayloadProcessor.from("/", "*", delegate));
        processors.add(MatchingPayloadProcessor.from("", "*", delegate));
        processors.add(MatchingPayloadProcessor.from("", "*", delegate));
        for (PayloadProcessor payloadProcessor : processors) {
            payloadProcessor.processRequest(new Payload(null, null, null, null, null, null));
            assertEquals(1, requestCount.get());
            payloadProcessor.processRequest(new Payload(null, "someModelId", null, null, null, null));
            assertEquals(2, requestCount.get());
            payloadProcessor.processRequest(new Payload(null, null, null, "processRequest", null, null));
            assertEquals(3, requestCount.get());
            payloadProcessor.processRequest(new Payload(null, "someModelId", null, "processRequest", null, null));
            assertEquals(4, requestCount.get());
            delegate.reset();
        }
    }

    @Test
    void testPayloadProcessingModelFilter() {
        AtomicInteger requestCount = new AtomicInteger();
        AtomicInteger responseCount = new AtomicInteger();
        PayloadProcessor delegate = new DummyPayloadProcessor(requestCount, responseCount);
        MatchingPayloadProcessor payloadProcessor = MatchingPayloadProcessor.from("someModelId", null, delegate);
        payloadProcessor.processRequest(new Payload(null, null, null, null, null, null));
        assertEquals(0, requestCount.get());
        payloadProcessor.processRequest(new Payload(null, "someModelId", null, null, null, null));
        assertEquals(1, requestCount.get());
        payloadProcessor.processRequest(new Payload(null, null, null, "processRequest", null, null));
        assertEquals(1, requestCount.get());
        payloadProcessor.processRequest(new Payload(null, "someModelId", null, "processRequest", null, null));
        assertEquals(2, requestCount.get());
    }

    @Test
    void testPayloadProcessingMethodFilter() {
        AtomicInteger requestCount = new AtomicInteger();
        AtomicInteger responseCount = new AtomicInteger();
        PayloadProcessor delegate = new DummyPayloadProcessor(requestCount, responseCount);
        MatchingPayloadProcessor payloadProcessor = MatchingPayloadProcessor.from(null, "getName", delegate);
        payloadProcessor.processRequest(new Payload(null, null, null, null, null, null));
        assertEquals(0, requestCount.get());
        payloadProcessor.processRequest(new Payload(null, "someModelId", null, null, null, null));
        assertEquals(0, requestCount.get());
        payloadProcessor.processRequest(new Payload(null, null, null, "getName", null, null));
        assertEquals(1, requestCount.get());
        payloadProcessor.processRequest(new Payload(null, "someModelId", null, "getName", null, null));
        assertEquals(2, requestCount.get());
    }
}