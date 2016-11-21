package com.baidu.sqlengine.memory.unsafe.array;

import com.baidu.sqlengine.memory.unsafe.Platform;

public class ByteArrayMethods {

    private ByteArrayMethods() {
        // Private constructor, since this class only contains static methods.
    }

    /**
     * Returns the next number greater or equal num that is power of 2.
     */
    public static long nextPowerOf2(long num) {
        final long highBit = Long.highestOneBit(num);
        return (highBit == num) ? num : highBit << 1;
    }

    public static int roundNumberOfBytesToNearestWord(int numBytes) {
        int remainder = numBytes & 0x07;  // This is equivalent to `numBytes % 8`
        if (remainder == 0) {
            return numBytes;
        } else {
            return numBytes + (8 - remainder);
        }
    }

    /**
     * Optimized byte array equality check for byte arrays.
     *
     * @return true if the arrays are equal, false otherwise
     */
    public static boolean arrayEquals(
            Object leftBase, long leftOffset, Object rightBase, long rightOffset, final long length) {
        int i = 0;
        while (i <= length - 8) {
            if (Platform.getLong(leftBase, leftOffset + i) !=
                    Platform.getLong(rightBase, rightOffset + i)) {
                return false;
            }
            i += 8;
        }
        while (i < length) {
            if (Platform.getByte(leftBase, leftOffset + i) !=
                    Platform.getByte(rightBase, rightOffset + i)) {
                return false;
            }
            i += 1;
        }
        return true;
    }
}
