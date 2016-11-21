package com.baidu.sqlengine.memory.unsafe.map;

/**
 * Interface that defines how we can grow the size of a hash map when it is over a threshold.
 */
public interface HashMapGrowthStrategy {

    int nextCapacity(int currentCapacity);

    /**
     * Double the size of the hash map every time.
     */
    HashMapGrowthStrategy DOUBLING = new Doubling();

    class Doubling implements HashMapGrowthStrategy {
        @Override
        public int nextCapacity(int currentCapacity) {
            assert (currentCapacity > 0);
            // Guard against overflow
            return (currentCapacity * 2 > 0) ? (currentCapacity * 2) : Integer.MAX_VALUE;
        }
    }

}
