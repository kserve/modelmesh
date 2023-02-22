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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompositePayloadProcessor implements PayloadProcessor {

    private static final Logger logger = LoggerFactory.getLogger(CompositePayloadProcessor.class);

    private final List<PayloadProcessor> delegates;

    public CompositePayloadProcessor(List<PayloadProcessor> delegates) {
        this.delegates = delegates;
    }

    @Override
    public String getName() {
        return "composite";
    }

    @Override
    public void processRequest(Payload payload) {
        for (PayloadProcessor processor : this.delegates) {
            try {
                processor.processRequest(payload);
            } catch (Throwable t) {
                logger.error("PayloadProcessor {} failed processing request payload due to {}", processor.getName(), t.getMessage());
            }
        }
    }

    @Override
    public void processResponse(Payload payload) {
        for (PayloadProcessor processor : this.delegates) {
            try {
                processor.processResponse(payload);
            } catch (Throwable t) {
                logger.error("PayloadProcessor {} failed processing response payload due to {}", processor.getName(), t.getMessage());
            }
        }
    }
}