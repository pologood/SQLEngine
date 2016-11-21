package com.baidu.sqlengine.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.nio.ch.DirectBuffer;

/**
 * DirectByteBuffer池，可以分配任意指定大小的DirectByteBuffer，用完需要归还
 */
@SuppressWarnings("restriction")
public class DirectByteBufferPool implements BufferPool {
    private static final Logger LOGGER = LoggerFactory.getLogger("DirectByteBufferPool");
    public static final String LOCAL_BUF_THREAD_PREX = "$_";
    private ByteBufferPage[] allPages;
    private final int chunkSize;
    // private int prevAllocatedPage = 0;
    private AtomicInteger prevAllocatedPage;
    private final int pageSize;
    private final short pageCount;
    private final int conReadBuferChunk;

    public DirectByteBufferPool(int pageSize, short chunkSize, short pageCount, int conReadBuferChunk) {
        allPages = new ByteBufferPage[pageCount];
        this.chunkSize = chunkSize;
        this.pageSize = pageSize;
        this.pageCount = pageCount;
        this.conReadBuferChunk = conReadBuferChunk;
        this.prevAllocatedPage = new AtomicInteger(0);
        for (int i = 0; i < pageCount; i++) {
            allPages[i] = new ByteBufferPage(ByteBuffer.allocateDirect(pageSize), chunkSize);
        }
    }

    public BufferArray allocateArray() {
        return new BufferArray(this);
    }

    public ByteBuffer allocate(int size) {
        int theChunkCount = size / chunkSize + (size % chunkSize == 0 ? 0 : 1);
        int selectedPage = prevAllocatedPage.incrementAndGet() % allPages.length;
        ByteBuffer byteBuf = allocateBuffer(theChunkCount, 0, selectedPage);
        if (byteBuf == null) {
            byteBuf = allocateBuffer(theChunkCount, selectedPage, allPages.length);
        }
        return byteBuf;
    }

    public void recycle(ByteBuffer theBuf) {
        if (!(theBuf instanceof DirectBuffer)) {
            theBuf.clear();
            return;
        }
        boolean recycled = false;
        DirectBuffer thisNavBuf = (DirectBuffer) theBuf;
        int chunkCount = theBuf.capacity() / chunkSize;
        DirectBuffer parentBuf = (DirectBuffer) thisNavBuf.attachment();
        int startChunk = (int) ((thisNavBuf.address() - parentBuf.address()) / this.chunkSize);
        for (int i = 0; i < allPages.length; i++) {
            if ((recycled = allPages[i].recycleBuffer((ByteBuffer) parentBuf, startChunk, chunkCount) == true)) {
                break;
            }
        }
        if (recycled == false) {
            LOGGER.warn("warning ,not recycled buffer " + theBuf);
        }
    }

    private ByteBuffer allocateBuffer(int theChunkCount, int startPage, int endPage) {
        for (int i = startPage; i < endPage; i++) {
            ByteBuffer buffer = allPages[i].allocatChunk(theChunkCount);
            if (buffer != null) {
                prevAllocatedPage.getAndSet(i);
                return buffer;
            }
        }
        return null;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public int getPageSize() {
        return pageSize;
    }

    public short getPageCount() {
        return pageCount;
    }

    public long capacity() {
        return size();
    }

    public long size() {
        return (long) pageSize * chunkSize * pageCount;
    }

    public int getSharedOptsCount() {
        return 0;
    }

    public int getConReadBuferChunk() {
        return conReadBuferChunk;
    }

}
