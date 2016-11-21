package com.baidu.sqlengine;

import java.util.Date;

public final class SqlEngineShutdown {

    public static void main(String[] args) {
        SqlEngineServer server = SqlEngineServer.getInstance();
        server.shutdown();
        System.out.println(new Date() + ",server shutdown!");
    }

}