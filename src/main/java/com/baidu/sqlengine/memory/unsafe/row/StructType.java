package com.baidu.sqlengine.memory.unsafe.row;

import java.util.Map;

import javax.annotation.Nonnull;

import com.baidu.sqlengine.merge.ColMeta;
import com.baidu.sqlengine.merge.OrderCol;

public class StructType {

    private final Map<String, ColMeta> columToIndx;
    private final int fieldCount;

    private OrderCol[] orderCols = null;

    public StructType(@Nonnull Map<String, ColMeta> columToIndx, int fieldCount) {
        assert fieldCount >= 0;
        this.columToIndx = columToIndx;
        this.fieldCount = fieldCount;
    }

    public int length() {
        return fieldCount;
    }

    public Map<String, ColMeta> getColumToIndx() {
        return columToIndx;
    }

    public OrderCol[] getOrderCols() {
        return orderCols;
    }

    public void setOrderCols(OrderCol[] orderCols) {
        this.orderCols = orderCols;
    }

    public long apply(int i) {
        return 0;
    }
}
