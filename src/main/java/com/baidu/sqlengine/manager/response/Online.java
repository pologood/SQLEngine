package com.baidu.sqlengine.manager.response;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.manager.ManagerConnection;
import com.baidu.sqlengine.backend.mysql.protocol.OkPacket;

public class Online {

    private static final OkPacket ok = new OkPacket();
    static {
        ok.packetId = 1;
        ok.affectedRows = 1;
        ok.serverStatus = 2;
    }

    public static void execute(String stmt, ManagerConnection mc) {
        SqlEngineServer.getInstance().online();
        ok.write(mc);
    }

}