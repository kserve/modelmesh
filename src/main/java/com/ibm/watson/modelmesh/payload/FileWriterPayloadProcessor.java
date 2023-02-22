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

import java.io.FileOutputStream;
import java.io.IOException;

import io.netty.buffer.ByteBuf;

public class FileWriterPayloadProcessor extends PayloadDataProcessor {

    @Override
    public String getName() {
        return "file";
    }

    @Override
    public void processRequestPayload(Payload payload) {
        persist(payload, new java.util.Date().toInstant() + ".in");
    }

    @Override
    public void processResponsePayload(Payload payload) {
        persist(payload, new java.util.Date().toInstant() + ".out");
    }

    private static void persist(Payload payload, String path) {
        try (FileOutputStream fos = new FileOutputStream(path)) {
            fos.write((payload.getMetadata().toString() + "\n").getBytes());
            ByteBuf data = payload.getData();
            if (data.hasArray()) {
                fos.write(data.array());
            } else {
                byte[] dest = new byte[data.readableBytes()];
                data.slice().readBytes(dest);
                fos.write(dest);
            }
            fos.flush();
        } catch (IOException e) {
            // ignore it
        }
    }
}
