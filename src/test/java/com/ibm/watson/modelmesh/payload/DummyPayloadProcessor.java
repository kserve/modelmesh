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
import java.util.concurrent.atomic.AtomicInteger;

class DummyPayloadProcessor implements PayloadProcessor {

    private final AtomicInteger processCount;

    DummyPayloadProcessor() {
        this(new AtomicInteger(0));
    }

    DummyPayloadProcessor(AtomicInteger processCount) {
        this.processCount = processCount;
    }

    @Override
    public String getName() {
        return "dummy";
    }

    @Override
    public boolean process(Payload payload) {
        this.processCount.incrementAndGet();
        return false;
    }

    public AtomicInteger getProcessCount() {
        return processCount;
    }

    public void reset() {
        this.processCount.set(0);
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
