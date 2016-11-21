package com.baidu.sqlengine.manager.handler;

import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.manager.ManagerConnection;
import com.baidu.sqlengine.manager.response.ShowBackend;
import com.baidu.sqlengine.manager.response.ShowBackendOld;
import com.baidu.sqlengine.manager.response.ShowConnection;
import com.baidu.sqlengine.manager.response.ShowConnectionSQL;
import com.baidu.sqlengine.manager.response.ShowDataNode;
import com.baidu.sqlengine.manager.response.ShowDataSource;
import com.baidu.sqlengine.manager.response.ShowDatabase;
import com.baidu.sqlengine.manager.response.ShowHeartbeat;
import com.baidu.sqlengine.manager.response.ShowHelp;
import com.baidu.sqlengine.manager.response.ShowProcessor;
import com.baidu.sqlengine.manager.response.ShowSQL;
import com.baidu.sqlengine.manager.response.ShowSQLHigh;
import com.baidu.sqlengine.manager.response.ShowSQLLarge;
import com.baidu.sqlengine.manager.response.ShowSQLSlow;
import com.baidu.sqlengine.manager.response.ShowServer;
import com.baidu.sqlengine.manager.response.ShowSession;
import com.baidu.sqlengine.manager.response.ShowThreadPool;
import com.baidu.sqlengine.manager.response.ShowVersion;
import com.baidu.sqlengine.parser.manager.ManagerParseShow;
import com.baidu.sqlengine.server.handler.ShowCache;
import com.baidu.sqlengine.util.StringUtil;

public final class ShowHandler {

    public static void handle(String stmt, ManagerConnection c, int offset) {
        int rs = ManagerParseShow.parse(stmt, offset);
        switch (rs & 0xff) {
            case ManagerParseShow.CONNECTION:
                ShowConnection.execute(c);
                break;
            case ManagerParseShow.BACKEND:
                ShowBackend.execute(c);
                break;
            case ManagerParseShow.BACKEND_OLD:
                ShowBackendOld.execute(c);
                break;
            case ManagerParseShow.CONNECTION_SQL:
                ShowConnectionSQL.execute(c);
                break;
            case ManagerParseShow.DATABASE:
                ShowDatabase.execute(c);
                break;
            case ManagerParseShow.DATANODE:
                ShowDataNode.execute(c, null);
                break;
            case ManagerParseShow.DATANODE_WHERE: {
                String name = stmt.substring(rs >>> 8).trim();
                if (StringUtil.isEmpty(name)) {
                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                } else {
                    ShowDataNode.execute(c, name);
                }
                break;
            }
            case ManagerParseShow.DATASOURCE:
                ShowDataSource.execute(c, null);
                break;
            case ManagerParseShow.DATASOURCE_WHERE: {
                String name = stmt.substring(rs >>> 8).trim();
                if (StringUtil.isEmpty(name)) {
                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                } else {
                    ShowDataSource.execute(c, name);
                }
                break;
            }
            case ManagerParseShow.HELP:
                ShowHelp.execute(c);
                break;
            case ManagerParseShow.HEARTBEAT:
                ShowHeartbeat.response(c);
                break;
            case ManagerParseShow.PROCESSOR:
                ShowProcessor.execute(c);
                break;
            case ManagerParseShow.SERVER:
                ShowServer.execute(c);
                break;
            case ManagerParseShow.SQL:
                boolean isClearSql = Boolean.valueOf(stmt.substring(rs >>> 8).trim());
                ShowSQL.execute(c, isClearSql);
                break;
            case ManagerParseShow.SQL_SLOW:
                boolean isClearSlow = Boolean.valueOf(stmt.substring(rs >>> 8).trim());
                ShowSQLSlow.execute(c, isClearSlow);
                break;
            case ManagerParseShow.SQL_HIGH:
                boolean isClearHigh = Boolean.valueOf(stmt.substring(rs >>> 8).trim());
                ShowSQLHigh.execute(c, isClearHigh);
                break;
            case ManagerParseShow.SQL_LARGE:
                boolean isClearLarge = Boolean.valueOf(stmt.substring(rs >>> 8).trim());
                ShowSQLLarge.execute(c, isClearLarge);
                break;
            case ManagerParseShow.SLOW_DATANODE: {
                String name = stmt.substring(rs >>> 8).trim();
                if (StringUtil.isEmpty(name)) {
                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                } else {
                    // ShowSlow.dataNode(c, name);
                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                }
                break;
            }
            case ManagerParseShow.SLOW_SCHEMA: {
                String name = stmt.substring(rs >>> 8).trim();
                if (StringUtil.isEmpty(name)) {
                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                } else {
                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                    // ShowSlow.schema(c, name);
                }
                break;
            }
            case ManagerParseShow.THREADPOOL:
                ShowThreadPool.execute(c);
                break;
            case ManagerParseShow.CACHE:
                ShowCache.execute(c);
                break;
            case ManagerParseShow.SESSION:
                ShowSession.execute(c);
                break;
            case ManagerParseShow.VERSION:
                ShowVersion.execute(c);
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }
}