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

package com.ibm.watson.modelmesh.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CompositePayloadProcessorTest {

    @Test
    void testPayloadProcessing() {
        List<PayloadProcessor> delegates = new ArrayList<>();
        AtomicInteger integerOne = new AtomicInteger();
        delegates.add(new DummyPayloadProcessor(integerOne));
        AtomicInteger integerTwo = new AtomicInteger();
        delegates.add(new DummyPayloadProcessor(integerTwo));

        CompositePayloadProcessor payloadProcessor = new CompositePayloadProcessor(delegates);
        for (int i = 0; i < 10; i++) {
            payloadProcessor.process(new Payload(null, null, null, null, null));
        }
        assertEquals(10, integerOne.get());
        assertEquals(10, integerTwo.get());
    }
}