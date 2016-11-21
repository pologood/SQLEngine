package com.baidu.sqlengine.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.baidu.sqlengine.config.model.SystemConfig;
import com.baidu.sqlengine.memory.unsafe.Platform;
import com.baidu.sqlengine.memory.unsafe.memory.mm.DataNodeMemoryManager;
import com.baidu.sqlengine.memory.unsafe.memory.mm.MemoryManager;
import com.baidu.sqlengine.memory.unsafe.memory.mm.ResultMergeMemoryManager;
import com.baidu.sqlengine.memory.unsafe.storage.DataNodeDiskManager;
import com.baidu.sqlengine.memory.unsafe.storage.SerializerManager;
import com.baidu.sqlengine.memory.unsafe.utils.JavaUtils;
import com.baidu.sqlengine.memory.unsafe.utils.SqlEnginePropertyConf;
import com.google.common.annotations.VisibleForTesting;

/**
 * SqlEngine内存管理工具类
 * 规划为三部分内存:结果集处理内存,系统预留内存,网络处理内存
 * 其中网络处理内存部分全部为Direct Memory
 * 结果集内存分为Direct Memory 和 Heap Memory，但目前仅使用Direct Memory
 * 系统预留内存为 Heap Memory。
 * 系统运行时，必须设置-XX:MaxDirectMemorySize 和 -Xmx JVM参数
 * -Xmx1024m -Xmn512m -XX:MaxDirectMemorySize=2048m -Xss256K -XX:+UseParallelGC
 */

public class SqlEngineMemory {
    private static Logger LOGGER = LoggerFactory.getLogger(SqlEngineMemory.class);

    private final static double DIRECT_SAFETY_FRACTION = 0.7;
    private final long systemReserveBufferSize;

    private final long memoryPageSize;
    private final long spillsFileBufferSize;
    private final long resultSetBufferSize;
    private final int numCores;

    /**
     * 内存管理相关关键类
     */
    private final SqlEnginePropertyConf conf;
    private final MemoryManager resultMergeMemoryManager;
    private final DataNodeMemoryManager dataNodeMemoryManager;
    private final DataNodeDiskManager blockManager;
    private final SerializerManager serializerManager;
    private final SystemConfig system;

    public SqlEngineMemory(SystemConfig system, long totalNetWorkBufferSize)
            throws NoSuchFieldException, IllegalAccessException {

        this.system = system;

        LOGGER.info("useOffHeapForMerge = " + system.getUseOffHeapForMerge());
        LOGGER.info("memoryPageSize = " + system.getMemoryPageSize());
        LOGGER.info("spillsFileBufferSize = " + system.getSpillsFileBufferSize());
        LOGGER.info("useStreamOutput = " + system.getUseStreamOutput());
        LOGGER.info("systemReserveMemorySize = " + system.getSystemReserveMemorySize());
        LOGGER.info("totalNetWorkBufferSize = " + JavaUtils.bytesToString2(totalNetWorkBufferSize));
        LOGGER.info("dataNodeSortedTempDir = " + system.getDataNodeSortedTempDir());

        conf = new SqlEnginePropertyConf();
        numCores = Runtime.getRuntime().availableProcessors();

        this.systemReserveBufferSize = JavaUtils.byteStringAsBytes(system.getSystemReserveMemorySize());
        this.memoryPageSize = JavaUtils.byteStringAsBytes(system.getMemoryPageSize());

        this.spillsFileBufferSize = JavaUtils.byteStringAsBytes(system.getSpillsFileBufferSize());

        /**
         * 目前merge，order by ，limit 没有使用On Heap内存
         */
        long maxOnHeapMemory = (Platform.getMaxHeapMemory() - systemReserveBufferSize);

        assert maxOnHeapMemory > 0;

        resultSetBufferSize = (long) ((Platform.getMaxDirectMemory() - 2 * totalNetWorkBufferSize) * DIRECT_SAFETY_FRACTION);

        assert resultSetBufferSize > 0;

        /**
         * sqlEngine.merge.memory.offHeap.enabled
         * sqlEngine.buffer.pageSize
         * sqlEngine.memory.offHeap.size
         * sqlEngine.merge.file.buffer
         * sqlEngine.direct.output.result
         * sqlEngine.local.dir
         */

        if (system.getUseOffHeapForMerge() == 1) {
            conf.set("sqlEngine.memory.offHeap.enabled", "true");
        } else {
            conf.set("sqlEngine.memory.offHeap.enabled", "false");
        }

        if (system.getUseStreamOutput() == 1) {
            conf.set("sqlEngine.stream.output.result", "true");
        } else {
            conf.set("sqlEngine.stream.output.result", "false");
        }

        if (system.getMemoryPageSize() != null) {
            conf.set("sqlEngine.buffer.pageSize", system.getMemoryPageSize());
        } else {
            conf.set("sqlEngine.buffer.pageSize", "1m");
        }

        if (system.getSpillsFileBufferSize() != null) {
            conf.set("sqlEngine.merge.file.buffer", system.getSpillsFileBufferSize());
        } else {
            conf.set("sqlEngine.merge.file.buffer", "32k");
        }

        conf.set("sqlEngine.pointer.array.len", "8k")
                .set("sqlEngine.memory.offHeap.size", JavaUtils.bytesToString2(resultSetBufferSize));

        LOGGER.info("sqlEngine.memory.offHeap.size: " +
                JavaUtils.bytesToString2(resultSetBufferSize));

        resultMergeMemoryManager = new ResultMergeMemoryManager(conf, numCores, maxOnHeapMemory);

        dataNodeMemoryManager = new DataNodeMemoryManager(resultMergeMemoryManager, 1);

        serializerManager = new SerializerManager();

        blockManager = new DataNodeDiskManager(conf, true, serializerManager);

    }

    @VisibleForTesting
    public SqlEngineMemory() throws NoSuchFieldException, IllegalAccessException {
        this.system = null;
        this.systemReserveBufferSize = 0;
        this.memoryPageSize = 0;
        this.spillsFileBufferSize = 0;
        conf = new SqlEnginePropertyConf();
        numCores = Runtime.getRuntime().availableProcessors();

        long maxOnHeapMemory = (Platform.getMaxHeapMemory());
        assert maxOnHeapMemory > 0;

        resultSetBufferSize = (long) ((Platform.getMaxDirectMemory()) * DIRECT_SAFETY_FRACTION);

        assert resultSetBufferSize > 0;
        /**
         * sqlEngine.memory.offHeap.enabled
         * sqlEngine.buffer.pageSize
         * sqlEngine.memory.offHeap.size
         * sqlEngine.testing.memory
         * sqlEngine.merge.file.buffer
         * sqlEngine.direct.output.result
         * sqlEngine.local.dir
         */
        conf.set("sqlEngine.memory.offHeap.enabled", "true")
                .set("sqlEngine.pointer.array.len", "8K")
                .set("sqlEngine.buffer.pageSize", "1m")
                .set("sqlEngine.memory.offHeap.size", JavaUtils.bytesToString2(resultSetBufferSize))
                .set("sqlEngine.stream.output.result", "false");

        LOGGER.info("sqlEngine.memory.offHeap.size: " + JavaUtils.bytesToString2(resultSetBufferSize));

        resultMergeMemoryManager =
                new ResultMergeMemoryManager(conf, numCores, maxOnHeapMemory);

        dataNodeMemoryManager = new DataNodeMemoryManager(resultMergeMemoryManager, 1);

        serializerManager = new SerializerManager();

        blockManager = new DataNodeDiskManager(conf, true, serializerManager);

    }

    public SqlEnginePropertyConf getConf() {
        return conf;
    }

    public int getNumCores() {
        return numCores;
    }

    public long getResultSetBufferSize() {
        return resultSetBufferSize;
    }

    public MemoryManager getResultMergeMemoryManager() {
        return resultMergeMemoryManager;
    }

    public DataNodeMemoryManager getDataNodeMemoryManager() {
        return dataNodeMemoryManager;
    }

    public SerializerManager getSerializerManager() {
        return serializerManager;
    }

    public DataNodeDiskManager getBlockManager() {
        return blockManager;
    }

    public long getSystemReserveBufferSize() {
        return systemReserveBufferSize;
    }

    public long getMemoryPageSize() {
        return memoryPageSize;
    }

    public long getSpillsFileBufferSize() {
        return spillsFileBufferSize;
    }

}
