package com.baidu.sqlengine.server.sqlcmd;

import com.baidu.sqlengine.backend.BackendConnection;
import com.baidu.sqlengine.server.NonBlockingSession;

/**
 * sql command like set xxxx ,only return OK /Err Pacakage,can't return restult set
 */
public interface SQLCtrlCommand {

    boolean isAutoClearSessionCons();

    boolean releaseConOnErr();

    boolean relaseConOnOK();

    void sendCommand(NonBlockingSession session, BackendConnection con);

    /**
     * 收到错误数据包的响应处理
     */
    void errorResponse(NonBlockingSession session, byte[] err, int total, int failed);

    /**
     * 收到OK数据包的响应处理
     */
    void okResponse(NonBlockingSession session, byte[] ok);

}
