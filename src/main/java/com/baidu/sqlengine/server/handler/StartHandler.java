package com.baidu.sqlengine.server.handler;

import com.baidu.sqlengine.server.ServerConnection;
import com.baidu.sqlengine.server.parser.ServerParse;
import com.baidu.sqlengine.server.parser.ServerParseStart;

public final class StartHandler {
    private static final byte[] AC_OFF = new byte[] {7, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0};

    public static void handle(String stmt, ServerConnection c, int offset) {
        switch (ServerParseStart.parse(stmt, offset)) {
            case ServerParseStart.TRANSACTION:
                if (c.isAutocommit()) {
                    c.setAutocommit(false);
                    c.write(c.writeToBuffer(AC_OFF, c.allocate()));
                } else {
                    c.getSession().commit();
                }
                break;
            default:
                c.execute(stmt, ServerParse.START);
        }
    }

}