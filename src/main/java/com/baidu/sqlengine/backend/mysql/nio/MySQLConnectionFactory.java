package com.baidu.sqlengine.backend.mysql.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NetworkChannel;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.backend.mysql.nio.handler.ResponseHandler;
import com.baidu.sqlengine.config.model.DBHostConfig;
import com.baidu.sqlengine.net.NIOConnector;
import com.baidu.sqlengine.net.factory.BackendConnectionFactory;

public class MySQLConnectionFactory extends BackendConnectionFactory {
    @SuppressWarnings({"unchecked", "rawtypes"})
    public MySQLConnection make(MySQLDataSource pool, ResponseHandler handler, String schema) throws IOException {

        DBHostConfig dsc = pool.getConfig();
        NetworkChannel channel = openSocketChannel();

        MySQLConnection c = new MySQLConnection(channel, pool.isReadNode());
        SqlEngineServer.getInstance().getConfig().setSocketParams(c, false);
        c.setHost(dsc.getIp());
        c.setPort(dsc.getPort());
        c.setUser(dsc.getUser());
        c.setPassword(dsc.getPassword());
        c.setSchema(schema);
        c.setHandler(new MySQLConnectionAuthenticator(c, handler));
        c.setPool(pool);
        c.setIdleTimeout(pool.getConfig().getIdleTimeout());

        ((NIOConnector) SqlEngineServer.getInstance().getConnector()).postConnect(c);
        return c;
    }

}