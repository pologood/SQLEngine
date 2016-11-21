package com.baidu.sqlengine.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.SqlEngineServer;

public final class NIOConnector extends Thread implements SocketConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(NIOConnector.class);
    public static final ConnectIdGenerator ID_GENERATOR = new ConnectIdGenerator();

    private final String name;
    private final Selector selector;
    private final BlockingQueue<AbstractConnection> connectQueue;
    private long connectCount;
    private final NIOReactorPool reactorPool;

    public NIOConnector(String name, NIOReactorPool reactorPool) throws IOException {
        super.setName(name);
        this.name = name;
        this.selector = Selector.open();
        this.reactorPool = reactorPool;
        this.connectQueue = new LinkedBlockingQueue<AbstractConnection>();
    }

    public long getConnectCount() {
        return connectCount;
    }

    public void postConnect(AbstractConnection c) {
        connectQueue.offer(c);
        selector.wakeup();
    }

    @Override
    public void run() {
        final Selector tSelector = this.selector;
        for (; ; ) {
            ++connectCount;
            try {
                tSelector.select(1000L);
                connect(tSelector);
                Set<SelectionKey> keys = tSelector.selectedKeys();
                try {
                    for (SelectionKey key : keys) {
                        Object att = key.attachment();
                        if (att != null && key.isValid() && key.isConnectable()) {
                            finishConnect(key, att);
                        } else {
                            key.cancel();
                        }
                    }
                } finally {
                    keys.clear();
                }
            } catch (Exception e) {
                LOGGER.warn(name, e);
            }
        }
    }

    private void connect(Selector selector) {
        AbstractConnection c = null;
        while ((c = connectQueue.poll()) != null) {
            try {
                SocketChannel channel = (SocketChannel) c.getChannel();
                channel.register(selector, SelectionKey.OP_CONNECT, c);
                channel.connect(new InetSocketAddress(c.host, c.port));

            } catch (Exception e) {
                c.close(e.toString());
            }
        }
    }

    private void finishConnect(SelectionKey key, Object att) {
        BackendAIOConnection c = (BackendAIOConnection) att;
        try {
            if (finishConnect(c, (SocketChannel) c.channel)) {
                clearSelectionKey(key);
                c.setId(ID_GENERATOR.getId());
                NIOProcessor processor = SqlEngineServer.getInstance()
                        .nextProcessor();
                c.setProcessor(processor);
                NIOReactor reactor = reactorPool.getNextReactor();
                reactor.postRegister(c);
                c.onConnectfinish();
            }
        } catch (Exception e) {
            clearSelectionKey(key);
            c.close(e.toString());
            c.onConnectFailed(e);

        }
    }

    private boolean finishConnect(AbstractConnection c, SocketChannel channel)
            throws IOException {
        if (channel.isConnectionPending()) {
            channel.finishConnect();

            c.setLocalPort(channel.socket().getLocalPort());
            return true;
        } else {
            return false;
        }
    }

    private void clearSelectionKey(SelectionKey key) {
        if (key.isValid()) {
            key.attach(null);
            key.cancel();
        }
    }

    /**
     * 后端连接ID生成器
     */
    public static class ConnectIdGenerator {

        private static final long MAX_VALUE = Long.MAX_VALUE;
        private AtomicLong connectId = new AtomicLong(0);

        public long getId() {
            return connectId.incrementAndGet();
        }
    }

}
