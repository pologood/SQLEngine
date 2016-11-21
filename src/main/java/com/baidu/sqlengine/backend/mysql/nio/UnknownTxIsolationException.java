package com.baidu.sqlengine.backend.mysql.nio;

/**
 * 未知事物隔离级别异常
 */
public class UnknownTxIsolationException extends RuntimeException {
    private static final long serialVersionUID = -3911059999308980358L;

    public UnknownTxIsolationException() {
        super();
    }

    public UnknownTxIsolationException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnknownTxIsolationException(String message) {
        super(message);
    }

    public UnknownTxIsolationException(Throwable cause) {
        super(cause);
    }

}