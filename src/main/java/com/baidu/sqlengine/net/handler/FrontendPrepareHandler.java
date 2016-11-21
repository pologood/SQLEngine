package com.baidu.sqlengine.net.handler;

/**
 * SQL预处理处理器
 */
public interface FrontendPrepareHandler {

    void prepare(String sql);

    void sendLongData(byte[] data);

    void reset(byte[] data);

    void execute(byte[] data);

    void close(byte[] data);

    void clear();

}