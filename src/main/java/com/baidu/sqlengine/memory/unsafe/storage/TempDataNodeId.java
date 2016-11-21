package com.baidu.sqlengine.memory.unsafe.storage;

public class TempDataNodeId extends ConnectionId {

    private String uuid;

    public TempDataNodeId(String uuid) {
        super();
        this.name = uuid;
        this.uuid = uuid;
    }

    @Override
    public String getBlockName() {
        return "temp_local_" + uuid;
    }
}
