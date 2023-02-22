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

class DummyPayloadProcessor implements PayloadProcessor {

    private final AtomicInteger requestCount;
    private final AtomicInteger responseCount;

    DummyPayloadProcessor() {
        this(new AtomicInteger(0), new AtomicInteger(0));
    }

    DummyPayloadProcessor(AtomicInteger requestCount, AtomicInteger responseCount) {
        this.requestCount = requestCount;
        this.responseCount = responseCount;
    }

    @Override
    public String getName() {
        return "dummy";
    }

    @Override
    public void processRequest(Payload payload) {
        this.requestCount.incrementAndGet();
    }

    public void processResponse(Payload payload) {
        this.responseCount.incrementAndGet();
    }

    public AtomicInteger getRequestCount() {
        return requestCount;
    }

    public AtomicInteger getResponseCount() {
        return responseCount;
    }
}
