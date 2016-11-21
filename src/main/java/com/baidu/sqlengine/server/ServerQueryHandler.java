package com.baidu.sqlengine.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.net.handler.FrontendQueryHandler;
import com.baidu.sqlengine.backend.mysql.protocol.OkPacket;
import com.baidu.sqlengine.server.handler.*;
import com.baidu.sqlengine.server.parser.ServerParse;

public class ServerQueryHandler implements FrontendQueryHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerQueryHandler.class);

    private final ServerConnection source;
    protected Boolean readOnly;

    public void setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
    }

    public ServerQueryHandler(ServerConnection source) {
        this.source = source;
    }

    @Override
    public void query(String sql) {

        ServerConnection c = this.source;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(new StringBuilder().append(c).append(sql).toString());
        }

        int rs = ServerParse.parse(sql);
        int sqlType = rs & 0xff;

        switch (sqlType) {
            //explain sql
            case ServerParse.EXPLAIN:
                ExplainHandler.handle(sql, c, rs >>> 8);
                break;
            //explain2 datanode=? sql=?
            case ServerParse.EXPLAIN2:
                Explain2Handler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.SET:
                SetHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.SHOW:
                ShowHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.SELECT:
                SelectHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.START:
                StartHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.BEGIN:
                BeginHandler.handle(sql, c);
                break;
            //不支持oracle的savepoint事务回退点
            case ServerParse.SAVEPOINT:
                SavepointHandler.handle(sql, c);
                break;
            case ServerParse.KILL:
                KillHandler.handle(sql, rs >>> 8, c);
                break;
            //不支持KILL_Query
            case ServerParse.KILL_QUERY:
                LOGGER.warn(new StringBuilder().append("Unsupported command:").append(sql).toString());
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported command");
                break;
            case ServerParse.USE:
                UseHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.COMMIT:
                c.commit();
                break;
            case ServerParse.ROLLBACK:
                c.rollback();
                break;
            case ServerParse.HELP:
                LOGGER.warn(new StringBuilder().append("Unsupported command:").append(sql).toString());
                c.writeErrMessage(ErrorCode.ER_SYNTAX_ERROR, "Unsupported command");
                break;
            case ServerParse.MYSQL_CMD_COMMENT:
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            case ServerParse.MYSQL_COMMENT:
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            case ServerParse.LOAD_DATA_INFILE_SQL:
                break;
            default:
                if (readOnly) {
                    LOGGER.warn(new StringBuilder().append("User readonly:").append(sql).toString());
                    c.writeErrMessage(ErrorCode.ER_USER_READ_ONLY, "User readonly");
                    break;
                }
                c.execute(sql, rs & 0xff);
        }
    }

}
