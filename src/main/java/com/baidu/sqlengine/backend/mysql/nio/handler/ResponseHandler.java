package com.baidu.sqlengine.backend.mysql.nio.handler;

import java.util.List;

import com.baidu.sqlengine.backend.BackendConnection;

public interface ResponseHandler {

    /**
     * 无法获取连接
     *
     * @param e
     * @param conn
     */
    void connectionError(Throwable e, BackendConnection conn);

    /**
     * 已获得有效连接的响应处理
     */
    void connectionAcquired(BackendConnection conn);

    /**
     * 收到错误数据包的响应处理
     */
    void errorResponse(byte[] err, BackendConnection conn);

    /**
     * 收到OK数据包的响应处理
     */
    void okResponse(byte[] ok, BackendConnection conn);

    /**
     * 收到字段数据包结束的响应处理
     */
    void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof,
                          BackendConnection conn);

    /**
     * 收到行数据包的响应处理
     */
    void rowResponse(byte[] row, BackendConnection conn);

    /**
     * 收到行数据包结束的响应处理
     */
    void rowEofResponse(byte[] eof, BackendConnection conn);

    /**
     * 写队列为空，可以写数据了
     */
    void writeQueueAvailable();

    /**
     * on connetion close event
     */
    void connectionClose(BackendConnection conn, String reason);

}