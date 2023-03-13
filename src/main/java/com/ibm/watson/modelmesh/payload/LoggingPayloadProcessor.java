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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PayloadProcessor} that logs {@link Payload}s to a logger (INFO level).
 */
public class LoggingPayloadProcessor implements PayloadProcessor {

    private final static Logger LOG = LoggerFactory.getLogger(LoggingPayloadProcessor.class);

    @Override
    public String getName() {
        return "logger";
    }

    @Override
    public boolean process(Payload payload) {
        LOG.info("Payload: {}", payload);
        return false;
    }

}
