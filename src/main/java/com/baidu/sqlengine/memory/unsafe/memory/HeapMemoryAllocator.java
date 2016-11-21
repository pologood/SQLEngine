package com.baidu.sqlengine.memory.unsafe.memory;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.annotation.concurrent.GuardedBy;

import com.baidu.sqlengine.memory.unsafe.Platform;

/**
 * A simple {@link MemoryAllocator} that can allocate up to 16GB using a JVM long primitive array.
 */
public class HeapMemoryAllocator implements MemoryAllocator {

    @GuardedBy("this")
    private final Map<Long, LinkedList<WeakReference<MemoryBlock>>> bufferPoolsBySize =
            new HashMap<Long, LinkedList<WeakReference<MemoryBlock>>>();

    private static final int POOLING_THRESHOLD_BYTES = 1024 * 1024;

    /**
     * Returns true if allocations of the given size should go through the pooling mechanism and
     * false otherwise.
     */
    private boolean shouldPool(long size) {
        // Very small allocations are less likely to benefit from pooling.
        return size >= POOLING_THRESHOLD_BYTES;
    }

    @Override
    public MemoryBlock allocate(long size) throws OutOfMemoryError {
        if (shouldPool(size)) {
            synchronized(this) {
                final LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
                if (pool != null) {
                    while (!pool.isEmpty()) {
                        final WeakReference<MemoryBlock> blockReference = pool.pop();
                        final MemoryBlock memory = blockReference.get();
                        if (memory != null) {
                            assert (memory.size() == size);
                            return memory;
                        }
                    }
                    bufferPoolsBySize.remove(size);
                }
            }
        }
        long[] array = new long[(int) ((size + 7) / 8)];
        return new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
    }

    @Override
    public void free(MemoryBlock memory) {
        final long size = memory.size();
        if (shouldPool(size)) {
            synchronized(this) {
                LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
                if (pool == null) {
                    pool = new LinkedList<WeakReference<MemoryBlock>>();
                    bufferPoolsBySize.put(size, pool);
                }
                pool.add(new WeakReference<MemoryBlock>(memory));
            }
        } else {
            // Do nothing
        }
    }
}
