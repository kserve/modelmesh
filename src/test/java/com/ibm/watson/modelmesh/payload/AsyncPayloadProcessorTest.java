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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class AsyncPayloadProcessorTest {

    @Test
    void testPayloadProcessing() {
        DummyPayloadProcessor dummyPayloadProcessor = new DummyPayloadProcessor();

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        AsyncPayloadProcessor payloadProcessor = new AsyncPayloadProcessor(dummyPayloadProcessor, 1, TimeUnit.NANOSECONDS, scheduler, 100);

        for (int i = 0; i < 10; i++) {
            payloadProcessor.process(new Payload("123", "456", null, null, null, null));
        }
        try {
            assertFalse(scheduler.awaitTermination(1, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            // ignore it
        }
        for (int i = 0; i < 10; i++) {
            payloadProcessor.process(new Payload("123", "456", null, null, null, null));
        }
        try {
            assertFalse(scheduler.awaitTermination(1, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            // ignore it
        }
        assertEquals(20, dummyPayloadProcessor.getProcessCount().get());
    }
}