package com.baidu.sqlengine.backend.mysql.nio.handler;

import java.util.List;

import com.baidu.sqlengine.backend.BackendConnection;

public class DelegateResponseHandler implements ResponseHandler {
    private final ResponseHandler target;

    public DelegateResponseHandler(ResponseHandler target) {
        if (target == null) {
            throw new IllegalArgumentException("delegate is null!");
        }
        this.target = target;
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        target.connectionAcquired(conn);
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        target.connectionError(e, conn);
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        target.okResponse(ok, conn);
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        target.errorResponse(err, conn);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {
        target.fieldEofResponse(header, fields, eof, conn);
    }

    @Override
    public void rowResponse(byte[] row, BackendConnection conn) {
        target.rowResponse(row, conn);
    }

    @Override
    public void rowEofResponse(byte[] eof, BackendConnection conn) {
        target.rowEofResponse(eof, conn);
    }

    @Override
    public void writeQueueAvailable() {
        target.writeQueueAvailable();

    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        target.connectionClose(conn, reason);
    }

}