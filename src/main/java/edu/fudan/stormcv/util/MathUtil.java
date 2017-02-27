package edu.fudan.stormcv.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class MathUtil {

    public static byte[] intToByteArray(int integer) throws IOException {
        ByteArrayOutputStream boutput = new ByteArrayOutputStream();
        DataOutputStream doutput = new DataOutputStream(boutput);
        doutput.writeInt(integer);
        return boutput.toByteArray();
    }

    public static int byteArrayToInt(byte[] b) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            int shift = (4 - 1 - i) * 8;
            value += (b[i] & 0x000000FF) << shift;
        }
        return value;
    }
}
