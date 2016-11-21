package com.baidu.sqlengine.manager.response;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.backend.datasource.PhysicalDBNode;
import com.baidu.sqlengine.backend.datasource.PhysicalDBPool;
import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.config.SqlEngineConfig;
import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.manager.ManagerConnection;
import com.baidu.sqlengine.backend.mysql.protocol.OkPacket;


public class ClearSlow {

    public static void dataNode(ManagerConnection c, String name) {
    	PhysicalDBNode dn = SqlEngineServer.getInstance().getConfig().getDataNodes().get(name);
    	PhysicalDBPool ds = null;
        if (dn != null && ((ds = dn.getDbPool())!= null)) {
           // ds.getSqlRecorder().clear();
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        } else {
            c.writeErrMessage(ErrorCode.ER_YES, "Invalid DataNode:" + name);
        }
    }

    public static void schema(ManagerConnection c, String name) {
        SqlEngineConfig conf = SqlEngineServer.getInstance().getConfig();
        SchemaConfig schema = conf.getSchemas().get(name);
        if (schema != null) {
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        } else {
            c.writeErrMessage(ErrorCode.ER_YES, "Invalid Schema:" + name);
        }
    }

}