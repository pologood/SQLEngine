package com.baidu.sqlengine.memory.unsafe.utils.sort;

import java.util.Iterator;

public class AbstractScalaRowIterator<T> implements Iterator<T> {

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T next() {
        return null;
    }

    @Override
    public void remove() {

    }
}
