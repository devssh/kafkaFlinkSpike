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

    public static float delay(float factor) {
        float wait = 0.0f;
        while (wait < 1000000) {
            for (int i = 0; i < 1000; i++) {
                for (int j = 0; j < 1000; j++) {
                    wait = wait + (1 / factor);
                }
            }
        }
        return wait;
    }
}
