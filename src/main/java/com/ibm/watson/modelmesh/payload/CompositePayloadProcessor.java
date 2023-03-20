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
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A composite {@link PayloadProcessor} that delegates processing to multiple delegate {@link PayloadProcessor}s (sequentially).
 */
public class CompositePayloadProcessor implements PayloadProcessor {

    private static final Logger logger = LoggerFactory.getLogger(CompositePayloadProcessor.class);

    private final List<PayloadProcessor> delegates;

    /**
     * If any of the delegate processors take ownership of the payload.
     *
     * @param delegates the delegate processors
     */
    public CompositePayloadProcessor(List<PayloadProcessor> delegates) {
        if (delegates.stream().anyMatch(PayloadProcessor::mayTakeOwnership)) {
            throw new IllegalArgumentException(
                "CompositePayloadProcessor can only be used with delegate processors that won't take ownership"
            );
        }
        this.delegates = delegates;
    }

    @Override
    public String getName() {
        return "composite:[" + delegates.stream().map(PayloadProcessor::getName).collect(Collectors.joining()) + "]";
    }

    @Override
    public boolean process(Payload payload) {
        for (PayloadProcessor processor : delegates) {
            boolean consumed = false;
            try {
                consumed = processor.process(payload);
            } catch (Throwable t) {
                logger.error("PayloadProcessor {} failed processing payload {}", processor.getName(), payload, t);
            }
            if (consumed) {
                throw new RuntimeException("PayloadProcessor " + processor.getName()
                        + " unexpectedly took ownership of the payload");
            }
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        for (PayloadProcessor processor : this.delegates) {
            processor.close();
        }
    }
}
