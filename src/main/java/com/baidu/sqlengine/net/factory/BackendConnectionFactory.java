package com.baidu.sqlengine.net.factory;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;

import com.baidu.sqlengine.SqlEngineServer;

public abstract class BackendConnectionFactory {

    protected NetworkChannel openSocketChannel() throws IOException {
        SocketChannel channel = null;
        channel = SocketChannel.open();
        channel.configureBlocking(false);
        return channel;
    }

}