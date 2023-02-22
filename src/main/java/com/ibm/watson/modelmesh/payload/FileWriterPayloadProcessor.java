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
