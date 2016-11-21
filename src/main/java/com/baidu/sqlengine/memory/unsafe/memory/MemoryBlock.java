package com.baidu.sqlengine.memory.unsafe.memory;

import javax.annotation.Nullable;

import com.baidu.sqlengine.memory.unsafe.Platform;

/**
 * A consecutive block of memory, starting at a {@link MemoryLocation} with a fixed size.
 */
public class MemoryBlock extends MemoryLocation {

    private final long length;

    /**
     * Optional page number; used when this MemoryBlock represents a page allocated by a
     * DataNodeMemoryManager. This field is public so that it can be modified by the DataNodeMemoryManager,
     * which lives in a different package.
     */
    public int pageNumber = -1;

    public MemoryBlock(@Nullable Object obj, long offset, long length) {
        super(obj, offset);
        this.length = length;
    }

    /**
     * Returns the size of the memory block.
     */
    public long size() {
        return length;
    }

    /**
     * Creates a memory block pointing to the memory used by the long array.
     */
    public static MemoryBlock fromLongArray(final long[] array) {
        return new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, array.length * 8);
    }

}
