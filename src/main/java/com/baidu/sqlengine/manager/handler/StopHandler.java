package com.baidu.sqlengine.manager.handler;

import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.manager.ManagerConnection;
import com.baidu.sqlengine.manager.response.StopHeartbeat;
import com.baidu.sqlengine.parser.manager.ManagerParseStop;

public final class StopHandler {

    public static void handle(String stmt, ManagerConnection c, int offset) {
        switch (ManagerParseStop.parse(stmt, offset)) {
            case ManagerParseStop.HEARTBEAT:
                StopHeartbeat.execute(stmt, c);
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }
}