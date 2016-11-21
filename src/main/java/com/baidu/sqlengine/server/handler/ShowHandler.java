package com.baidu.sqlengine.server.handler;

import com.baidu.sqlengine.server.ServerConnection;
import com.baidu.sqlengine.server.parser.ServerParse;
import com.baidu.sqlengine.server.parser.ServerParseShow;
import com.baidu.sqlengine.server.response.ShowDatabases;
import com.baidu.sqlengine.server.response.ShowFullTables;
import com.baidu.sqlengine.server.response.ShowSqlEngineCluster;
import com.baidu.sqlengine.server.response.ShowSqlEngineStatus;
import com.baidu.sqlengine.server.response.ShowTables;
import com.baidu.sqlengine.util.StringUtil;

public final class ShowHandler {

    public static void handle(String stmt, ServerConnection c, int offset) {

        // 排除 “ ` ” 符号
        stmt = StringUtil.replaceChars(stmt, "`", null);

        int type = ServerParseShow.parse(stmt, offset);
        switch (type) {
            case ServerParseShow.DATABASES:
                ShowDatabases.response(c);
                break;
            case ServerParseShow.TABLES:
                ShowTables.response(c, stmt, type);
                break;
            case ServerParseShow.FULLTABLES:
                ShowFullTables.response(c, stmt, type);
                break;
            case ServerParseShow.SQL_ENGINE_STATUS:
                ShowSqlEngineStatus.response(c);
                break;
            case ServerParseShow.SQL_ENGINE_CLUSTER:
                ShowSqlEngineCluster.response(c);
                break;
            default:
                c.execute(stmt, ServerParse.SHOW);
        }
    }

}