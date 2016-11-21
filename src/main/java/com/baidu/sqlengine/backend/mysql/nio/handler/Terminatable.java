package com.baidu.sqlengine.backend.mysql.nio.handler;

public interface Terminatable {
    void terminate(Runnable runnable);
}