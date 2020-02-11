package com.etl;

import java.util.Arrays;

public class StringUtils {
    public static byte[] stringToBytes(String some) {
        return some.getBytes();
    }

    public static String bytesToString(byte[] some) {
        return Arrays.toString(some);
    }

    public static long timestampNow() {
        return System.currentTimeMillis();
    }
}
