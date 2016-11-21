
package com.baidu.sqlengine.util;

import com.baidu.sqlengine.util.tool.Pair;

public final class PairUtil {
    private static final int DEFAULT_INDEX = -1;

    /**
     * <pre>
     * 将名字和索引用进行分割 当src = "offer_group[4]", l='[', r=']'时，
     * 返回的Piar<String,Integer>("offer", 4);
     * 当src = "offer_group", l='[', r=']'时， 
     * 返回Pair<String, Integer>("offer",-1);
     * </pre>
     */
    public static Pair<String, Integer> splitIndex(String src, char l, char r) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return new Pair<String, Integer>("", DEFAULT_INDEX);
        }
        if (src.charAt(length - 1) != r) {
            return new Pair<String, Integer>(src, DEFAULT_INDEX);
        }
        int offset = src.lastIndexOf(l);
        if (offset == -1) {
            return new Pair<String, Integer>(src, DEFAULT_INDEX);
        }
        int index = DEFAULT_INDEX;
        try {
            index = Integer.parseInt(src.substring(offset + 1, length - 1));
        } catch (NumberFormatException e) {
            return new Pair<String, Integer>(src, DEFAULT_INDEX);
        }
        return new Pair<String, Integer>(src.substring(0, offset), index);
    }

}