package com.baidu.sqlengine.backend.mysql.nio.handler;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.backend.BackendConnection;

public class NewConnectionRespHandler implements ResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(NewConnectionRespHandler.class);

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.warn(conn + " connectionError " + e);
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        //
        conn.release();
        LOGGER.info("connectionAcquired " + conn);
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        LOGGER.warn("caught error resp: " + conn + " " + new String(err));
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        LOGGER.info("okResponse: " + conn);

    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields,
                                 byte[] eof, BackendConnection conn) {
        LOGGER.info("fieldEofResponse: " + conn);
    }

    @Override
    public void rowResponse(byte[] row, BackendConnection conn) {
        LOGGER.info("rowResponse: " + conn);
    }

    @Override
    public void rowEofResponse(byte[] eof, BackendConnection conn) {
        LOGGER.info("rowEofResponse: " + conn);

    }

    @Override
    public void writeQueueAvailable() {

    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {

    }

}