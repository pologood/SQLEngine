package com.baidu.sqlengine.manager.handler;

import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.manager.ManagerConnection;
import com.baidu.sqlengine.parser.manager.ManagerParseSelect;

public final class SelectHandler {

    public static void handle(String stmt, ManagerConnection c, int offset) {
        switch (ManagerParseSelect.parse(stmt, offset)) {
//            case VERSION_COMMENT:
//                SelectVersionComment.execute(c);
//                break;
//            case SESSION_AUTO_INCREMENT:
//                SelectSessionAutoIncrement.execute(c);
//                break;
//            case SESSION_TX_READ_ONLY:
//                SelectSessionTxReadOnly.execute(c);
//                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}