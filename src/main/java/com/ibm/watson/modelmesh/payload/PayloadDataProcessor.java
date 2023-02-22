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

public abstract class PayloadDataProcessor implements PayloadProcessor{

    @Override
    public void processRequest(Payload payload) {
        processPayload(payload, p -> {
            try {
                processRequestPayload(p);
                return true;
            } catch (Throwable t) {
                return false;
            }
        });
    }

    private void processPayload(Payload payload, Function<Payload, Boolean> function) {
        ByteBuf data = payload.getData();
        try {
            data.retain();
            function.apply(payload);
        } finally {
            data.release();
        }
    }

    protected abstract void processRequestPayload(Payload payload);

    @Override
    public void processResponse(Payload payload) {
        processPayload(payload, p -> {
            try {
                processResponsePayload(p);
                return true;
            } catch (Throwable t) {
                return false;
            }
        });
    }

    protected abstract void processResponsePayload(Payload payload);
}
