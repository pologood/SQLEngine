package com.baidu.sqlengine.server;

import java.io.IOException;
import java.nio.channels.NetworkChannel;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.config.SqlEnginePrivileges;
import com.baidu.sqlengine.config.model.SystemConfig;
import com.baidu.sqlengine.net.FrontendConnection;
import com.baidu.sqlengine.net.factory.FrontendConnectionFactory;
import com.baidu.sqlengine.server.handler.ServerPrepareHandler;

public class ServerConnectionFactory extends FrontendConnectionFactory {

    @Override
    protected FrontendConnection getConnection(NetworkChannel channel) throws IOException {
        SystemConfig sys = SqlEngineServer.getInstance().getConfig().getSystem();
        ServerConnection c = new ServerConnection(channel);
        SqlEngineServer.getInstance().getConfig().setSocketParams(c, true);
        c.setPrivileges(SqlEnginePrivileges.instance());
        c.setQueryHandler(new ServerQueryHandler(c));
        c.setPrepareHandler(new ServerPrepareHandler(c));
        c.setTxIsolation(sys.getTxIsolation());
        c.setSession(new NonBlockingSession(c));
        return c;
    }

}