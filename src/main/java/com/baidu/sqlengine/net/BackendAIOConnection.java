package com.baidu.sqlengine.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.NetworkChannel;

import com.baidu.sqlengine.backend.BackendConnection;

public abstract class BackendAIOConnection extends AbstractConnection implements
        BackendConnection {

    protected boolean isFinishConnect;

    public BackendAIOConnection(NetworkChannel channel) {
        super(channel);
    }

    public void register() throws IOException {
        this.asynRead();
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void discardClose(String reason) {
        //跨节点处理,中断后端连接时关闭
    }

    public abstract void onConnectFailed(Throwable e);

    public boolean finishConnect() throws IOException {
        localPort = ((InetSocketAddress) channel.getLocalAddress()).getPort();
        isFinishConnect = true;
        return true;
    }

    public void setProcessor(NIOProcessor processor) {
        super.setProcessor(processor);
        processor.addBackend(this);
    }

    @Override
    public String toString() {
        return "BackendConnection [id=" + id + ", host=" + host + ", port="
                + port + ", localPort=" + localPort + "]";
    }
}