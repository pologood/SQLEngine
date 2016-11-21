package com.baidu.sqlengine.memory.unsafe.storage;

public abstract class ConnectionId {
    protected String name;

    public abstract String getBlockName();

    @Override
    public boolean equals(Object arg0) {
        return super.equals(arg0);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return super.toString();
    }

}
