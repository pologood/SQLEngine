package com.baidu.sqlengine.net.handler;

import java.util.concurrent.Executor;

import com.baidu.sqlengine.net.NIOHandler;

public abstract class BackendAsyncHandler implements NIOHandler {

    protected void offerData(byte[] data, Executor executor) {
        handleData(data);
    }

    protected abstract void offerDataError();

    protected abstract void handleData(byte[] data);

}