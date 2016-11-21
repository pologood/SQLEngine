package com.baidu.sqlengine.memory.unsafe.memory;

import com.baidu.sqlengine.memory.unsafe.Platform;

/**
 * A simple {@link MemoryAllocator} that uses {@code Unsafe} to allocate off-heap memory.
 */
public class UnsafeMemoryAllocator implements MemoryAllocator {

    @Override
    public MemoryBlock allocate(long size) throws OutOfMemoryError {
        long address = Platform.allocateMemory(size);
        return new MemoryBlock(null, address, size);
    }

    @Override
    public void free(MemoryBlock memory) {
        assert (memory.obj == null) :
                "baseObject not null; are you trying to use the off-heap allocator to free on-heap memory?";
        Platform.freeMemory(memory.offset);
    }
}
