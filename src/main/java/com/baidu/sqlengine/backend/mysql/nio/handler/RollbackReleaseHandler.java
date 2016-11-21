package com.baidu.sqlengine.backend.mysql.nio.handler;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.backend.BackendConnection;

public class RollbackReleaseHandler implements ResponseHandler {
    private static final Logger logger = LoggerFactory.getLogger(RollbackReleaseHandler.class);

    public RollbackReleaseHandler() {
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        logger.error("unexpected invocation: connectionAcquired from rollback-release");
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {

    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        conn.quit();
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        logger.debug("autocomit is false,but no commit or rollback ,so sqlEngine rollbacked backend conn " + conn);
        conn.release();
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {
    }

    @Override
    public void rowResponse(byte[] row, BackendConnection conn) {
    }

    @Override
    public void rowEofResponse(byte[] eof, BackendConnection conn) {

    }

    @Override
    public void writeQueueAvailable() {

    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {

    }

}