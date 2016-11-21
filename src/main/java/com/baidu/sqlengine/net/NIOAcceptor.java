package com.baidu.sqlengine.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.net.factory.FrontendConnectionFactory;

public final class NIOAcceptor extends Thread implements SocketAcceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(NIOAcceptor.class);
    private static final AcceptIdGenerator ID_GENERATOR = new AcceptIdGenerator();

    private final int port;
    private final Selector selector;
    private final ServerSocketChannel serverChannel;
    private final FrontendConnectionFactory factory;
    private long acceptCount;
    private final NIOReactorPool reactorPool;

    public NIOAcceptor(String name, String bindIp, int port,
                       FrontendConnectionFactory factory, NIOReactorPool reactorPool)
            throws IOException {
        super.setName(name);
        this.port = port;
        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.configureBlocking(false);
        /** 设置TCP属性 */
        serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        serverChannel.setOption(StandardSocketOptions.SO_RCVBUF, 1024 * 16 * 2);
        // backlog=100
        serverChannel.bind(new InetSocketAddress(bindIp, port), 100);
        this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        this.factory = factory;
        this.reactorPool = reactorPool;
    }

    public int getPort() {
        return port;
    }

    public long getAcceptCount() {
        return acceptCount;
    }

    @Override
    public void run() {
        final Selector tSelector = this.selector;
        for (; ; ) {
            ++acceptCount;
            try {
                tSelector.select(1000L);
                Set<SelectionKey> keys = tSelector.selectedKeys();
                try {
                    for (SelectionKey key : keys) {
                        if (key.isValid() && key.isAcceptable()) {
                            accept();
                        } else {
                            key.cancel();
                        }
                    }
                } finally {
                    keys.clear();
                }
            } catch (Exception e) {
                LOGGER.warn(getName(), e);
            }
        }
    }

    private void accept() {
        SocketChannel channel = null;
        try {
            channel = serverChannel.accept();
            channel.configureBlocking(false);
            FrontendConnection c = factory.make(channel);
            c.setAccepted(true);
            c.setId(ID_GENERATOR.getId());
            NIOProcessor processor = (NIOProcessor) SqlEngineServer.getInstance().nextProcessor();
            c.setProcessor(processor);

            NIOReactor reactor = reactorPool.getNextReactor();
            reactor.postRegister(c);

        } catch (Exception e) {
            LOGGER.warn(getName(), e);
            closeChannel(channel);
        }
    }

    private static void closeChannel(SocketChannel channel) {
        if (channel == null) {
            return;
        }
        Socket socket = channel.socket();
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                LOGGER.error("closeChannelError", e);
            }
        }
        try {
            channel.close();
        } catch (IOException e) {
            LOGGER.error("closeChannelError", e);
        }
    }

    /**
     * 前端连接ID生成器
     *
     * @author sqlEngine
     */
    private static class AcceptIdGenerator {

        private static final long MAX_VALUE = 0xffffffffL;

        private long acceptId = 0L;
        private final Object lock = new Object();

        private long getId() {
            synchronized(lock) {
                if (acceptId >= MAX_VALUE) {
                    acceptId = 0L;
                }
                return ++acceptId;
            }
        }
    }

}