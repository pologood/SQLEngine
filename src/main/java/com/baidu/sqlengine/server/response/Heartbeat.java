package com.baidu.sqlengine.server.response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.backend.mysql.protocol.ErrorPacket;
import com.baidu.sqlengine.backend.mysql.protocol.HeartbeatPacket;
import com.baidu.sqlengine.backend.mysql.protocol.OkPacket;
import com.baidu.sqlengine.server.ServerConnection;
import com.baidu.sqlengine.util.TimeUtil;

public class Heartbeat {

    private static final Logger HEARTBEAT = LoggerFactory.getLogger("heartbeat");

    public static void response(ServerConnection c, byte[] data) {
        HeartbeatPacket hp = new HeartbeatPacket();
        hp.read(data);
        if (SqlEngineServer.getInstance().isOnline()) {
            OkPacket ok = new OkPacket();
            ok.packetId = 1;
            ok.affectedRows = hp.id;
            ok.serverStatus = 2;
            ok.write(c);
            if (HEARTBEAT.isInfoEnabled()) {
                HEARTBEAT.info(responseMessage("OK", c, hp.id));
            }
        } else {
            ErrorPacket error = new ErrorPacket();
            error.packetId = 1;
            error.errno = ErrorCode.ER_SERVER_SHUTDOWN;
            error.message = String.valueOf(hp.id).getBytes();
            error.write(c);
            if (HEARTBEAT.isInfoEnabled()) {
                HEARTBEAT.info(responseMessage("ERROR", c, hp.id));
            }
        }
    }

    private static String responseMessage(String action, ServerConnection c, long id) {
        return new StringBuilder("RESPONSE:").append(action).append(", id=").append(id).append(", host=")
                .append(c.getHost()).append(", port=").append(c.getPort()).append(", time=")
                .append(TimeUtil.currentTimeMillis()).toString();
    }

}