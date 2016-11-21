package com.baidu.sqlengine.route;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.backend.mysql.protocol.EOFPacket;
import com.baidu.sqlengine.backend.mysql.protocol.FieldPacket;
import com.baidu.sqlengine.backend.mysql.protocol.ResultSetHeaderPacket;
import com.baidu.sqlengine.backend.mysql.protocol.RowDataPacket;
import com.baidu.sqlengine.parser.druid.DruidSequenceHandler;
import com.baidu.sqlengine.server.ServerConnection;
import com.baidu.sqlengine.util.StringUtil;

public class SqlEngineSequnceProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlEngineSequnceProcessor.class);
    private LinkedBlockingQueue<SessionSQLPair> seqSQLQueue = new LinkedBlockingQueue<SessionSQLPair>();
    private volatile boolean running = true;

    public SqlEngineSequnceProcessor() {
        new ExecuteThread().start();
    }

    public void addNewSql(SessionSQLPair pair) {
        seqSQLQueue.add(pair);
    }

    private void outRawData(ServerConnection sc, String value) {
        byte packetId = 0;
        int fieldCount = 1;
        ByteBuffer byteBuf = sc.allocate();
        ResultSetHeaderPacket headerPkg = new ResultSetHeaderPacket();
        headerPkg.fieldCount = fieldCount;
        headerPkg.packetId = ++packetId;

        byteBuf = headerPkg.write(byteBuf, sc, true);
        FieldPacket fieldPkg = new FieldPacket();
        fieldPkg.packetId = ++packetId;
        fieldPkg.name = StringUtil.encode("SEQUNCE", sc.getCharset());
        byteBuf = fieldPkg.write(byteBuf, sc, true);

        EOFPacket eofPckg = new EOFPacket();
        eofPckg.packetId = ++packetId;
        byteBuf = eofPckg.write(byteBuf, sc, true);

        RowDataPacket rowDataPkg = new RowDataPacket(fieldCount);
        rowDataPkg.packetId = ++packetId;
        rowDataPkg.add(StringUtil.encode(value, sc.getCharset()));
        byteBuf = rowDataPkg.write(byteBuf, sc, true);
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        byteBuf = lastEof.write(byteBuf, sc, true);

        // write buffer
        sc.write(byteBuf);
    }

    private void executeSeq(SessionSQLPair pair) {
        try {
            //使用Druid解析器实现sequence处理
            DruidSequenceHandler sequenceHandler = new DruidSequenceHandler(SqlEngineServer
                    .getInstance().getConfig().getSystem().getSequnceHandlerType());

            String charset = pair.session.getSource().getCharset();
            String executeSql = sequenceHandler.getExecuteSql(pair.sql, charset == null ? "utf-8" : charset);

            pair.session.getSource().routeEndExecuteSQL(executeSql, pair.type, pair.schema);
        } catch (Exception e) {
            LOGGER.error("SqlEngineSequenceProcessor.executeSeq(SesionSQLPair)", e);
            pair.session.getSource().writeErrMessage(ErrorCode.ER_YES, "sqlEngine sequnce err." + e);
            return;
        }
    }

    public void shutdown() {
        running = false;
    }

    class ExecuteThread extends Thread {

        public ExecuteThread() {
            setDaemon(true); // 设置为后台线程,防止throw RuntimeExecption进程仍然存在的问题
        }

        public void run() {
            while (running) {
                try {
                    SessionSQLPair pair = seqSQLQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (pair != null) {
                        executeSeq(pair);
                    }
                } catch (Exception e) {
                    LOGGER.warn("SqlEngineSequenceProcessor$ExecutorThread", e);
                }
            }
        }
    }
}
