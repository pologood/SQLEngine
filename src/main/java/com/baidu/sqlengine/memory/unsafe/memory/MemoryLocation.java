package com.baidu.sqlengine.memory.unsafe.memory;

import javax.annotation.Nullable;

/**
 * A memory location. Tracked either by a memory address (with off-heap allocation),
 * or by an offset from a JVM object (in-heap allocation).
 */
public class MemoryLocation {

    @Nullable
    Object obj;

    long offset;

    public MemoryLocation(@Nullable Object obj, long offset) {
        this.obj = obj;
        this.offset = offset;
    }

    public MemoryLocation() {
        this(null, 0);
    }

    public void setObjAndOffset(Object newObj, long newOffset) {
        this.obj = newObj;
        this.offset = newOffset;
    }

    public final Object getBaseObject() {
        return obj;
    }

    public final long getBaseOffset() {
        return offset;
    }
}
