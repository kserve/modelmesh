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
        AtomicInteger counter = new AtomicInteger();
        PayloadProcessor delegate = new DummyPayloadProcessor(counter);
        MatchingPayloadProcessor payloadProcessor = MatchingPayloadProcessor.from(null, null, delegate);
        payloadProcessor.process(new Payload("123", "456", null, null, null, null));
        assertEquals(1, counter.get());
        payloadProcessor.process(new Payload("456", "456", null, null, null, null));
        assertEquals(2, counter.get());
        payloadProcessor.process(new Payload("789", "456", null, null, null, null));
        assertEquals(3, counter.get());
        payloadProcessor.process(new Payload("abc", "456", "processRequest", null, null, null));
        assertEquals(4, counter.get());
    }

    @Test
    void testPayloadProcessingAnySpecialChars() {
        AtomicInteger counter = new AtomicInteger();
        DummyPayloadProcessor delegate = new DummyPayloadProcessor(counter);
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
            payloadProcessor.process(new Payload("123", "456", null, null, null, null));
            assertEquals(1, counter.get());
            payloadProcessor.process(new Payload("456", "456", null, null, null, null));
            assertEquals(2, counter.get());
            payloadProcessor.process(new Payload("789", "456", "processRequest", null, null, null));
            assertEquals(3, counter.get());
            payloadProcessor.process(new Payload("abc", "456", "processRequest", null, null, null));
            assertEquals(4, counter.get());
            delegate.reset();
        }
    }

    @Test
    void testPayloadProcessingModelFilter() {
        AtomicInteger counter = new AtomicInteger();
        DummyPayloadProcessor delegate = new DummyPayloadProcessor(counter);
        MatchingPayloadProcessor payloadProcessor = MatchingPayloadProcessor.from("someModelId", null, delegate);
        payloadProcessor.process(new Payload("123", "nogo", null, null, null, null));
        assertEquals(0, counter.get());
        payloadProcessor.process(new Payload("456", "someModelId", null, null, null, null));
        assertEquals(1, counter.get());
        payloadProcessor.process(new Payload( "789", "nogo", "processRequest", null, null, null));
        assertEquals(1, counter.get());
        payloadProcessor.process(new Payload( "abc", "someModelId", "processRequest", null, null, null));
        assertEquals(2, counter.get());
    }

    @Test
    void testPayloadProcessingMethodFilter() {
        AtomicInteger counter = new AtomicInteger();
        DummyPayloadProcessor delegate = new DummyPayloadProcessor(counter);
        MatchingPayloadProcessor payloadProcessor = MatchingPayloadProcessor.from(null, "getName", delegate);
        payloadProcessor.process(new Payload("123", "456", null, null, null, null));
        assertEquals(0, counter.get());
        payloadProcessor.process(new Payload("456", "456", null, null, null, null));
        assertEquals(0, counter.get());
        payloadProcessor.process(new Payload("789", "456", "getName", null, null, null));
        assertEquals(1, counter.get());
        payloadProcessor.process(new Payload("abc", "456", "getName", null, null, null));
        assertEquals(2, counter.get());
        payloadProcessor.process(new Payload("def", "456", "filteredMethod", null, null, null));
        assertEquals(2, counter.get());
    }
}