package com.baidu.sqlengine.manager.handler;

import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.manager.ManagerConnection;
import com.baidu.sqlengine.manager.response.ReloadConfig;
import com.baidu.sqlengine.parser.manager.ManagerParseReload;

public final class ReloadHandler {

    public static void handle(String stmt, ManagerConnection c, int offset) {
        int rs = ManagerParseReload.parse(stmt, offset);
        switch (rs) {
            case ManagerParseReload.CONFIG:
                ReloadConfig.execute(c, false);
                break;
            case ManagerParseReload.CONFIG_ALL:
                ReloadConfig.execute(c, true);
                break;
            case ManagerParseReload.ROUTE:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}