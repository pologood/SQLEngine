package com.baidu.sqlengine.net.factory;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.NetworkChannel;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.net.FrontendConnection;

public abstract class FrontendConnectionFactory {
    protected abstract FrontendConnection getConnection(NetworkChannel channel) throws IOException;

    public FrontendConnection make(NetworkChannel channel) throws IOException {
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);

        FrontendConnection c = getConnection(channel);
        SqlEngineServer.getInstance().getConfig().setSocketParams(c, true);
        return c;
    }

}