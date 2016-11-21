package com.baidu.sqlengine.manager.handler;

import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.manager.ManagerConnection;
import com.baidu.sqlengine.manager.response.ClearSlow;
import com.baidu.sqlengine.parser.manager.ManagerParseClear;
import com.baidu.sqlengine.util.StringUtil;

public class ClearHandler {

    public static void handle(String stmt, ManagerConnection c, int offset) {
        int rs = ManagerParseClear.parse(stmt, offset);
        switch (rs & 0xff) {
            case ManagerParseClear.SLOW_DATANODE: {
                String name = stmt.substring(rs >>> 8).trim();
                if (StringUtil.isEmpty(name)) {
                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                } else {
                    ClearSlow.dataNode(c, name);
                }
                break;
            }
            case ManagerParseClear.SLOW_SCHEMA: {
                String name = stmt.substring(rs >>> 8).trim();
                if (StringUtil.isEmpty(name)) {
                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                } else {
                    ClearSlow.schema(c, name);
                }
                break;
            }
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }
}