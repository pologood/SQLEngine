package com.baidu.sqlengine.server.handler;

import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.server.ServerConnection;

public final class SavepointHandler {

    public static void handle(String stmt, ServerConnection c) {
        c.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported statement");
    }

}