package com.ibm.watson.modelmesh;

import com.google.common.base.Strings;
import com.ibm.watson.modelmesh.api.ModelInfo;
import com.ibm.watson.modelmesh.api.RegisterModelRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProtoSplicerTest {

    static final String LONG = Strings.repeat("abcd", 35); // > 128 UTF8 bytes

    static final String[] testData = {
            // before, target
            "some-existing-string", "smaller",
            "small-string", "another-string-which-is-longer",
            "same-strings", "same-strings",
            "same-length-abce", "efgh-same-length",
            "little", LONG,
            LONG, "little",
            null, "little",
            null, LONG
    };

    static final Object[] targetField = {
            "modelid", new int[] { 1 },
            "path", new int[] { 2, 2 }
    };


    @Test
    public void testIdExtractor() throws Exception {
        // various permutations of before/after/field existence
        for (int j = 0; j < targetField.length / 2; j++) {
            String field = (String) targetField[2 * j];
            int[] fieldPath = (int[]) targetField[2 * j + 1];

            for (int i = 0; i < testData.length / 2; i++) {
                String before = testData[2 * i];

                ModelInfo.Builder mib = ModelInfo.newBuilder()
                        .setType("typpe")
                        .setKey("dontchangeme");
                if ("path".equals(field)) {
                    if (before != null) {
                        mib.setPath(before);
                    } else {
                        mib.clearPath();
                    }
                }

                RegisterModelRequest.Builder amrb = RegisterModelRequest.newBuilder()
                        .setModelId("123").setSync(true).setModelInfo(mib);

                if ("modelid".equals(field)) {
                    if (before != null) {
                        amrb.setModelId(before);
                    } else {
                        amrb.clearModelId();
                    }
                }

                ByteBuf bb = Unpooled.wrappedBuffer(amrb.build().toByteArray());
                assertEquals(before, ProtoSplicer.extractId(bb, fieldPath), "i=" + i + ", j=" + j);
            }
        }
    }


    @Test
    public void testIdSplicer() throws Exception {
        // various permutations of before/after/field existence
        for (int k = 0; k < 2; k++) {
            for (int j = 0; j < targetField.length / 2; j++) {
                String field = (String) targetField[2 * j];
                int[] fieldPath = (int[]) targetField[2 * j + 1];

                for (int i = 0; i < testData.length / 2; i++) {
                    String before = testData[2 * i], after = testData[2 * i + 1];

                    ModelInfo.Builder mib = null;
                    if (k == 0) {
                        mib = ModelInfo.newBuilder()
                                .setType("typpe")
                                .setKey("dontchangeme");
                        if ("path".equals(field)) {
                            if (before != null) {
                                mib.setPath(before);
                            } else {
                                mib.clearPath();
                            }
                        }

                    }
                    RegisterModelRequest.Builder amrb = RegisterModelRequest.newBuilder()
                            .setModelId("123").setSync(true);

                    if (mib != null) {
                        amrb.setModelInfo(mib);
                    }

                    if ("modelid".equals(field)) {
                        if (before != null) {
                            amrb.setModelId(before);
                        } else {
                            amrb.clearModelId();
                        }
                    }
                    RegisterModelRequest amrBefore = amrb.build();

                    if ("modelid".equals(field)) {
                        amrb.setModelId(after);
                    } else if ("path".equals(field)) {
                        if (mib == null) {
                            mib = ModelInfo.newBuilder();
                        }
                        mib.setPath(after);
                        amrb.setModelInfo(mib.build());
                    }
                    RegisterModelRequest amrTarget = amrb.build();

                    ByteBuf bb = Unpooled.wrappedBuffer(amrBefore.toByteArray());

                    assertEquals(1, bb.refCnt());

                    ByteBuf newBb = ProtoSplicer.spliceId(bb, fieldPath, after.getBytes(StandardCharsets.UTF_8));

                    RegisterModelRequest amrAfter = RegisterModelRequest.parseFrom(new ByteBufInputStream(newBb));

                    newBb.release();

                    assertEquals(0, newBb.refCnt());
                    assertEquals(0, bb.refCnt());

                    assertEquals(amrTarget, amrAfter, "i=" + i + ", j=" + j + " k=" + k);
                }
            }
        }
    }


}
