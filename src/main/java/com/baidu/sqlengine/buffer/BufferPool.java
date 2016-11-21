package com.baidu.sqlengine.buffer;

import java.nio.ByteBuffer;

/**
 * 缓冲池
 */
public interface BufferPool {

    ByteBuffer allocate(int size);

    void recycle(ByteBuffer theBuf);

    long capacity();

    long size();

    int getConReadBuferChunk();

    int getSharedOptsCount();

    int getChunkSize();

    BufferArray allocateArray();
}
