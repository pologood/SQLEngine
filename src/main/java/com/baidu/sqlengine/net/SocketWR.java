package com.baidu.sqlengine.net;

import java.io.IOException;

public abstract class SocketWR {
    public abstract void asynRead() throws IOException;

    public abstract void doNextWriteCheck();
}
