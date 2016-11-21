package com.baidu.sqlengine.backend.mysql.nio;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.backend.mysql.ByteUtil;
import com.baidu.sqlengine.backend.mysql.nio.handler.ResponseHandler;
import com.baidu.sqlengine.backend.mysql.protocol.EOFPacket;
import com.baidu.sqlengine.backend.mysql.protocol.ErrorPacket;
import com.baidu.sqlengine.backend.mysql.protocol.OkPacket;
import com.baidu.sqlengine.net.handler.BackendAsyncHandler;

/**
 * life cycle: from connection establish to close <br/>
 */
public class MySQLConnectionHandler extends BackendAsyncHandler {
    private static final Logger logger = LoggerFactory.getLogger(MySQLConnectionHandler.class);
    private static final int RESULT_STATUS_INIT = 0;
    private static final int RESULT_STATUS_HEADER = 1;
    private static final int RESULT_STATUS_FIELD_EOF = 2;

    private final MySQLConnection source;
    private volatile int resultStatus;
    private volatile byte[] header;
    private volatile List<byte[]> fields;

    /**
     * life cycle: one SQL execution
     */
    private volatile ResponseHandler responseHandler;

    public MySQLConnectionHandler(MySQLConnection source) {
        this.source = source;
        this.resultStatus = RESULT_STATUS_INIT;
    }

    public void connectionError(Throwable e) {
        if (responseHandler != null) {
            responseHandler.connectionError(e, source);
        }

    }

    public MySQLConnection getSource() {
        return source;
    }

    @Override
    public void handle(byte[] data) {
        offerData(data, source.getProcessor().getExecutor());
    }

    @Override
    protected void offerDataError() {
        resultStatus = RESULT_STATUS_INIT;
        throw new RuntimeException("offer data error!");
    }

    @Override
    protected void handleData(byte[] data) {
        switch (resultStatus) {
            case RESULT_STATUS_INIT:
                switch (data[4]) {
                    case OkPacket.FIELD_COUNT:
                        handleOkPacket(data);
                        break;
                    case ErrorPacket.FIELD_COUNT:
                        handleErrorPacket(data);
                        break;
                    default:
                        resultStatus = RESULT_STATUS_HEADER;
                        header = data;
                        fields = new ArrayList<byte[]>((int) ByteUtil.readLength(data,
                                4));
                }
                break;
            case RESULT_STATUS_HEADER:
                switch (data[4]) {
                    case ErrorPacket.FIELD_COUNT:
                        resultStatus = RESULT_STATUS_INIT;
                        handleErrorPacket(data);
                        break;
                    case EOFPacket.FIELD_COUNT:
                        resultStatus = RESULT_STATUS_FIELD_EOF;
                        handleFieldEofPacket(data);
                        break;
                    default:
                        fields.add(data);
                }
                break;
            case RESULT_STATUS_FIELD_EOF:
                switch (data[4]) {
                    case ErrorPacket.FIELD_COUNT:
                        resultStatus = RESULT_STATUS_INIT;
                        handleErrorPacket(data);
                        break;
                    case EOFPacket.FIELD_COUNT:
                        resultStatus = RESULT_STATUS_INIT;
                        handleRowEofPacket(data);
                        break;
                    default:
                        handleRowPacket(data);
                }
                break;
            default:
                throw new RuntimeException("unknown status!");
        }
    }

    public void setResponseHandler(ResponseHandler responseHandler) {
        this.responseHandler = responseHandler;
    }

    /**
     * OK数据包处理
     */
    private void handleOkPacket(byte[] data) {
        ResponseHandler respHand = responseHandler;
        if (respHand != null) {
            respHand.okResponse(data, source);
        }
    }

    /**
     * ERROR数据包处理
     */
    private void handleErrorPacket(byte[] data) {
        ResponseHandler respHand = responseHandler;
        if (respHand != null) {
            respHand.errorResponse(data, source);
        } else {
            closeNoHandler();
        }
    }

    /**
     * 字段数据包结束处理
     */
    private void handleFieldEofPacket(byte[] data) {
        ResponseHandler respHand = responseHandler;
        if (respHand != null) {
            respHand.fieldEofResponse(header, fields, data, source);
        } else {
            closeNoHandler();
        }
    }

    /**
     * 行数据包处理
     */
    private void handleRowPacket(byte[] data) {
        ResponseHandler respHand = responseHandler;
        if (respHand != null) {
            respHand.rowResponse(data, source);
        } else {
            closeNoHandler();

        }
    }

    private void closeNoHandler() {
        if (!source.isClosedOrQuit()) {
            source.close("no handler");
            logger.warn("no handler bind in this con " + this + " client:" + source);
        }
    }

    /**
     * 行数据包结束处理
     */
    private void handleRowEofPacket(byte[] data) {
        if (responseHandler != null) {
            responseHandler.rowEofResponse(data, source);
        } else {
            closeNoHandler();
        }
    }

}