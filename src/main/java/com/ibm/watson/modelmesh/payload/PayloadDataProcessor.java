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

import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PayloadProcessor} abstract implementation for {@link PayloadProcessor}s that need to access {@link ByteBuf}s
 * contained in {@link Payload}s.
 * This class takes care of retaining and releasing such {@link ByteBuf}s during payload processing.
 * Extensions of this class should not modify {@link ByteBuf}s contained in {@link Payload}s.
 */
public abstract class PayloadDataProcessor implements PayloadProcessor{

    private static final Logger logger = LoggerFactory.getLogger(PayloadDataProcessor.class);

    @Override
    public void processRequest(Payload payload) {
        processPayload(payload, p -> {
            try {
                processRequestPayload(p);
                return true;
            } catch (Throwable t) {
                logger.error("Error while processing response payload {}: {}", payload, t.getMessage());
                return false;
            }
        });
    }

    private void processPayload(Payload payload, Function<Payload, Boolean> function) {
        ByteBuf data = payload.getData();
        try {
            if (data != null) {
                data.retain();
            }
            function.apply(payload);
        } finally {
            if (data != null) {
                try {
                    data.release();
                } catch (Throwable t) {
                    logger.debug("Payload data already released");
                }
            }
        }
    }

    /**
     * Process the request payload (read-only access).
     * The payload data should not be altered in any way.
     *
     * @param payload the payload
     */
    protected abstract void processRequestPayload(Payload payload);

    @Override
    public void processResponse(Payload payload) {
        processPayload(payload, p -> {
            try {
                processResponsePayload(p);
                return true;
            } catch (Throwable t) {
                logger.error("Error while processing response payload {}: {}", payload, t.getMessage());
                return false;
            }
        });
    }

    /**
     * Process the response payload (read-only access).
     * The payload data should not be altered in any way.
     *
     * @param payload the payload
     */
    protected abstract void processResponsePayload(Payload payload);
}
