package com.baidu.sqlengine.net;

public interface ClosableConnection {
    String getCharset();

    /**
     * 关闭连接
     */
    void close(String reason);

    boolean isClosed();

    void idleCheck();

    long getStartupTime();

    String getHost();

    int getPort();

    int getLocalPort();

    long getNetInBytes();

    long getNetOutBytes();
}