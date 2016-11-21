package com.baidu.sqlengine.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.backend.mysql.protocol.OkPacket;
import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.manager.handler.ClearHandler;
import com.baidu.sqlengine.manager.handler.ReloadHandler;
import com.baidu.sqlengine.manager.handler.RollbackHandler;
import com.baidu.sqlengine.manager.handler.SelectHandler;
import com.baidu.sqlengine.manager.handler.ShowHandler;
import com.baidu.sqlengine.manager.handler.StopHandler;
import com.baidu.sqlengine.manager.handler.SwitchHandler;
import com.baidu.sqlengine.manager.response.KillConnection;
import com.baidu.sqlengine.manager.response.Offline;
import com.baidu.sqlengine.manager.response.Online;
import com.baidu.sqlengine.net.handler.FrontendQueryHandler;
import com.baidu.sqlengine.parser.manager.ManagerParse;

public class ManagerQueryHandler implements FrontendQueryHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagerQueryHandler.class);
    private static final int SHIFT = 8;
    private final ManagerConnection source;
    protected Boolean readOnly;

    public ManagerQueryHandler(ManagerConnection source) {
        this.source = source;
    }

    public void setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    public void query(String sql) {
        ManagerConnection c = this.source;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(new StringBuilder().append(c).append(sql).toString());
        }
        int rs = ManagerParse.parse(sql);
        switch (rs & 0xff) {
            case ManagerParse.SELECT:
                SelectHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.SET:
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            case ManagerParse.SHOW:
                ShowHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.SWITCH:
                SwitchHandler.handler(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.KILL_CONN:
                KillConnection.response(sql, rs >>> SHIFT, c);
                break;
            case ManagerParse.OFFLINE:
                Offline.execute(sql, c);
                break;
            case ManagerParse.ONLINE:
                Online.execute(sql, c);
                break;
            case ManagerParse.STOP:
                StopHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.RELOAD:
                ReloadHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.ROLLBACK:
                RollbackHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.CLEAR:
                ClearHandler.handle(sql, c, rs >>> SHIFT);
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}