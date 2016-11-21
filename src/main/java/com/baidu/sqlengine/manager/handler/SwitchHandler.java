package com.baidu.sqlengine.manager.handler;

import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.manager.ManagerConnection;
import com.baidu.sqlengine.parser.manager.ManagerParseSwitch;

public final class SwitchHandler {

    public static void handler(String stmt, ManagerConnection c, int offset) {
        switch (ManagerParseSwitch.parse(stmt, offset)) {
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }
}