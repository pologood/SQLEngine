package com.baidu.sqlengine.backend.mysql.nio.handler;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.backend.BackendConnection;
import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.route.RouteResultsetNode;
import com.baidu.sqlengine.server.NonBlockingSession;

public class RollbackNodeHandler extends MultiNodeHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(RollbackNodeHandler.class);

    public RollbackNodeHandler(NonBlockingSession session) {
        super(session);
    }

    public void rollback() {
        final int initCount = session.getTargetCount();
        lock.lock();
        try {
            reset(initCount);
        } finally {
            lock.unlock();
        }
        if (session.closed()) {
            decrementCountToZero();
            return;
        }

        // 执行
        int started = 0;
        for (final RouteResultsetNode node : session.getTargetKeys()) {
            if (node == null) {
                LOGGER.error("null is contained in RoutResultsetNodes, source = "
                        + session.getSource());
                continue;
            }
            final BackendConnection conn = session.getTarget(node);

            if (conn != null) {
                boolean isClosed = conn.isClosedOrQuit();
                if (isClosed) {
                    session.getSource().writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR,
                            "receive rollback,but find backend con is closed or quit");
                    LOGGER.error(conn + "receive rollback,but fond backend con is closed or quit");
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("rollback job run for " + conn);
                }
                if (clearIfSessionClosed(session)) {
                    return;
                }
                conn.setResponseHandler(RollbackNodeHandler.this);
                conn.rollback();

                ++started;
            }
        }

        if (started < initCount && decrementCountBy(initCount - started)) {
            /**
             * assumption: only caused by front-end connection close. <br/>
             * Otherwise, packet must be returned to front-end
             */
            session.clearResources(true);
        }
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        if (decrementCountBy(1)) {
            // clear all resources
            session.clearResources(false);
            if (this.isFail() || session.closed()) {
                tryErrorFinished(true);
            } else {
                session.getSource().write(ok);
            }
        }
    }

    @Override
    public void rowEofResponse(byte[] eof, BackendConnection conn) {
        LOGGER.error(new StringBuilder().append("unexpected packet for ")
                .append(conn).append(" bound by ").append(session.getSource())
                .append(": field's eof").toString());
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        LOGGER.error("unexpected invocation: connectionAcquired from rollback");
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {
        LOGGER.error(new StringBuilder().append("unexpected packet for ")
                .append(conn).append(" bound by ").append(session.getSource())
                .append(": field's eof").toString());
    }

    @Override
    public void rowResponse(byte[] row, BackendConnection conn) {
        LOGGER.error(new StringBuilder().append("unexpected packet for ")
                .append(conn).append(" bound by ").append(session.getSource())
                .append(": field's eof").toString());
    }

    @Override
    public void writeQueueAvailable() {

    }

}
