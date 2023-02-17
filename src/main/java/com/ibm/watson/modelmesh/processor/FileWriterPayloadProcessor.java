package com.ibm.watson.modelmesh.processor;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import com.ibm.watson.modelmesh.ModelMeshApi;
import io.netty.buffer.ByteBuf;

public class FileWriterPayloadProcessor implements PayloadProcessor {

    @Override
    public String getName() {
        return "file";
    }

    @Override
    public void process(Payload payload) {
        persist(payload.getGRPCArgs(), new java.util.Date().toInstant() + ".in");
        persist(new Object[]{payload.getModelResponse()}, new java.util.Date().toInstant() + ".out");
    }

    private static void persist(Object[] args, String path) {
        try (FileOutputStream fos = new FileOutputStream(path)) {
            persistMultiple(args, fos);
            fos.flush();
        } catch (IOException e) {
            // ignore it
        }
    }

    private static void persistMultiple(Object[] args, FileOutputStream fos) throws IOException {
        for (Object arg : args) {
            if (arg == null) {
                continue;
            }
            if (arg.getClass().isArray()) {
                persistMultiple((Object[]) arg, fos);
            } else if (arg instanceof Collection) {
                persistMultiple(((Collection<?>) arg).toArray(), fos);
            } else {
                persistSingle(fos, arg);
            }
        }
    }

    private static void persistSingle(FileOutputStream fos, Object arg) throws IOException {
        if (arg instanceof ByteBuf) {
            if (((ByteBuf) arg).hasArray()) {
                fos.write(((ByteBuf) arg).array());
            } else {
                byte[] dest = new byte[((ByteBuf) arg).capacity()];
                ((ByteBuf) arg).writeBytes(dest);
                fos.write(dest);
            }
        } else if (arg instanceof ByteBuffer) {
            if (((ByteBuffer) arg).hasArray()) {
                fos.write(((ByteBuffer) arg).array());
            } else {
                byte[] dest = new byte[((ByteBuffer) arg).capacity()];
                ((ByteBuffer) arg).get(dest);
                fos.write(dest);
            }
        } else if (arg instanceof String) {
            fos.write(((String) arg).getBytes());
//        } else if (arg instanceof ModelMeshApi.ModelResponse) {
//            fos.write(((ModelMeshApi.ModelResponse) arg).metadata.toString().getBytes());
//            fos.write("\n".getBytes());
//            ByteBuf data = ((ModelMeshApi.ModelResponse) arg).data.copy();
//            if (data.hasArray()) {
//                fos.write(data.array());
//            } else {
//                byte[] dest = new byte[data.capacity()];
//                data.writeBytes(dest);
//                fos.write(dest);
//            }
//            fos.write(data.toString().getBytes());
        } else {
            fos.write(arg.toString().getBytes());
        }
        fos.write("\n".getBytes());
    }
}
