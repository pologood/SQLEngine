package com.baidu.sqlengine.net.handler;

import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.net.FrontendConnection;
import com.baidu.sqlengine.net.NIOHandler;
import com.baidu.sqlengine.backend.mysql.protocol.MySQLPacket;
import com.baidu.sqlengine.statistic.CommandCount;

/**
 * 前端命令处理器
 */
public class FrontendCommandHandler implements NIOHandler
{

    protected final FrontendConnection source;
    protected final CommandCount commands;

    public FrontendCommandHandler(FrontendConnection source)
    {
        this.source = source;
        this.commands = source.getProcessor().getCommands();
    }

    @Override
    public void handle(byte[] data)
    {
        switch (data[4])
        {
            case MySQLPacket.COM_INIT_DB:
                commands.doInitDB();
                source.initDB(data);
                break;
            case MySQLPacket.COM_QUERY:
                commands.doQuery();
                source.query(data);
                break;
            case MySQLPacket.COM_PING:
                commands.doPing();
                source.ping();
                break;
            case MySQLPacket.COM_QUIT:
                commands.doQuit();
                source.close("quit cmd");
                break;
            case MySQLPacket.COM_PROCESS_KILL:
                commands.doKill();
                source.kill(data);
                break;
            case MySQLPacket.COM_STMT_PREPARE:
                commands.doStmtPrepare();
                source.stmtPrepare(data);
                break;
            case MySQLPacket.COM_STMT_SEND_LONG_DATA:
            	commands.doStmtSendLongData();
            	source.stmtSendLongData(data);
            	break;
            case MySQLPacket.COM_STMT_RESET:
            	commands.doStmtReset();
            	source.stmtReset(data);
            	break;
            case MySQLPacket.COM_STMT_EXECUTE:
                commands.doStmtExecute();
                source.stmtExecute(data);
                break;
            case MySQLPacket.COM_STMT_CLOSE:
                commands.doStmtClose();
                source.stmtClose(data);
                break;
            case MySQLPacket.COM_HEARTBEAT:
                commands.doHeartbeat();
                source.heartbeat(data);
                break;
            default:
                     commands.doOther();
                     source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");

        }
    }

}