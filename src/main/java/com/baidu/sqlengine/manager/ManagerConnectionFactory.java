package com.baidu.sqlengine.manager;

import java.io.IOException;
import java.nio.channels.NetworkChannel;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.config.SqlEnginePrivileges;
import com.baidu.sqlengine.net.FrontendConnection;
import com.baidu.sqlengine.net.factory.FrontendConnectionFactory;

public class ManagerConnectionFactory extends FrontendConnectionFactory {

    @Override
    protected FrontendConnection getConnection(NetworkChannel channel) throws IOException {
        ManagerConnection c = new ManagerConnection(channel);
        SqlEngineServer.getInstance().getConfig().setSocketParams(c, true);
        c.setPrivileges(SqlEnginePrivileges.instance());
        c.setQueryHandler(new ManagerQueryHandler(c));
        return c;
    }

}