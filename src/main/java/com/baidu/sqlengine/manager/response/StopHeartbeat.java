package com.baidu.sqlengine.manager.response;

import java.util.Map;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.backend.datasource.PhysicalDBPool;
import com.baidu.sqlengine.manager.ManagerConnection;
import com.baidu.sqlengine.backend.mysql.protocol.OkPacket;
import com.baidu.sqlengine.parser.manager.ManagerParseStop;
import com.baidu.sqlengine.util.tool.Pair;
import com.baidu.sqlengine.util.FormatUtil;
import com.baidu.sqlengine.util.TimeUtil;

/**
 * 暂停数据节点心跳检测
 */
public final class StopHeartbeat {

    private static final Logger logger = LoggerFactory.getLogger(StopHeartbeat.class);

    public static void execute(String stmt, ManagerConnection c) {
        int count = 0;
        Pair<String[], Integer> keys = ManagerParseStop.getPair(stmt);
        if (keys.getKey() != null && keys.getValue() != null) {
            long time = keys.getValue().intValue() * 1000L;
            Map<String, PhysicalDBPool> dns = SqlEngineServer.getInstance().getConfig().getDataHosts();
            for (String key : keys.getKey()) {
            	PhysicalDBPool dn = dns.get(key);
                if (dn != null) {
                    dn.getSource().setHeartbeatRecoveryTime(TimeUtil.currentTimeMillis() + time);
                    ++count;
                    StringBuilder s = new StringBuilder();
                    s.append(dn.getHostName()).append(" stop heartbeat '");
                    logger.warn(s.append(FormatUtil.formatTime(time, 3)).append("' by manager.").toString());
                }
            }
        }
        OkPacket packet = new OkPacket();
        packet.packetId = 1;
        packet.affectedRows = count;
        packet.serverStatus = 2;
        packet.write(c);
    }

}