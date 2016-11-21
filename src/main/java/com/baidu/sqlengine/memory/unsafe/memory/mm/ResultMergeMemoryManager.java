package com.baidu.sqlengine.memory.unsafe.memory.mm;


import com.baidu.sqlengine.memory.unsafe.utils.SqlEnginePropertyConf;

public class ResultMergeMemoryManager extends MemoryManager {

    private long  maxOnHeapExecutionMemory;
    private int numCores;
    private SqlEnginePropertyConf conf;
    public ResultMergeMemoryManager(SqlEnginePropertyConf conf, int numCores, long onHeapExecutionMemory){
        super(conf,numCores,onHeapExecutionMemory);
        this.conf = conf;
        this.numCores = numCores;
        this.maxOnHeapExecutionMemory = onHeapExecutionMemory;
    }

    @Override
    protected  synchronized long acquireExecutionMemory(long numBytes,long taskAttemptId,MemoryMode memoryMode) throws InterruptedException {
        switch (memoryMode) {
            case ON_HEAP:
                return  onHeapExecutionMemoryPool.acquireMemory(numBytes,taskAttemptId);
            case OFF_HEAP:
                return  offHeapExecutionMemoryPool.acquireMemory(numBytes,taskAttemptId);
        }
        return 0L;
    }

}
