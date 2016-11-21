package com.baidu.sqlengine.backend.mysql.nio.handler;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.backend.BackendConnection;
import com.baidu.sqlengine.backend.datasource.PhysicalDBNode;
import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.config.SqlEngineConfig;
import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.backend.mysql.protocol.BinaryRowDataPacket;
import com.baidu.sqlengine.backend.mysql.protocol.ErrorPacket;
import com.baidu.sqlengine.backend.mysql.protocol.FieldPacket;
import com.baidu.sqlengine.backend.mysql.protocol.OkPacket;
import com.baidu.sqlengine.backend.mysql.protocol.RowDataPacket;
import com.baidu.sqlengine.route.RouteResultSet;
import com.baidu.sqlengine.route.RouteResultsetNode;
import com.baidu.sqlengine.server.NonBlockingSession;
import com.baidu.sqlengine.server.ServerConnection;
import com.baidu.sqlengine.server.parser.ServerParse;
import com.baidu.sqlengine.server.parser.ServerParseShow;
import com.baidu.sqlengine.server.response.ShowFullTables;
import com.baidu.sqlengine.server.response.ShowTables;
import com.baidu.sqlengine.statistic.stat.QueryResult;
import com.baidu.sqlengine.statistic.stat.QueryResultDispatcher;
import com.baidu.sqlengine.util.StringUtil;
import com.google.common.base.Strings;

public class SingleNodeHandler implements ResponseHandler, Terminatable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleNodeHandler.class);

    private final RouteResultsetNode node;
    private final RouteResultSet rrs;
    private final NonBlockingSession session;

    // only one thread access at one time no need lock
    private volatile byte packetId;
    private volatile ByteBuffer buffer;
    private volatile boolean isRunning;
    private Runnable terminateCallBack;
    private long startTime;
    private long netInBytes;
    private long netOutBytes;
    private long selectRows;
    private long affectedRows;

    private boolean prepared;
    private int fieldCount;
    private List<FieldPacket> fieldPackets = new ArrayList<FieldPacket>();

    private volatile boolean isDefaultNodeShowTable;
    private volatile boolean isDefaultNodeShowFullTable;
    private Set<String> shardingTablesSet;

    public SingleNodeHandler(RouteResultSet rrs, NonBlockingSession session) {
        this.rrs = rrs;
        this.node = rrs.getNodes()[0];

        if (node == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }

        if (session == null) {
            throw new IllegalArgumentException("session is null!");
        }

        this.session = session;
        ServerConnection source = session.getSource();
        String schema = source.getSchema();
        if (schema != null && ServerParse.SHOW == rrs.getSqlType()) {
            SchemaConfig schemaConfig = SqlEngineServer.getInstance().getConfig().getSchemas().get(schema);
            int type = ServerParseShow.tableCheck(rrs.getStatement(), 0);
            isDefaultNodeShowTable =
                    (ServerParseShow.TABLES == type && !Strings.isNullOrEmpty(schemaConfig.getDataNode()));
            isDefaultNodeShowFullTable =
                    (ServerParseShow.FULLTABLES == type && !Strings.isNullOrEmpty(schemaConfig.getDataNode()));
            if (isDefaultNodeShowTable) {
                shardingTablesSet = ShowTables.getTableSet(source, rrs.getStatement());

            } else if (isDefaultNodeShowFullTable) {
                shardingTablesSet = ShowFullTables.getTableSet(source, rrs.getStatement());
            }
        }

        if (rrs != null && rrs.getStatement() != null) {
            netInBytes += rrs.getStatement().getBytes().length;
        }

    }

    @Override
    public void terminate(Runnable callback) {
        boolean zeroReached = false;

        if (isRunning) {
            terminateCallBack = callback;
        } else {
            zeroReached = true;
        }

        if (zeroReached) {
            callback.run();
        }
    }

    private void endRunning() {
        Runnable callback = null;
        if (isRunning) {
            isRunning = false;
            callback = terminateCallBack;
            terminateCallBack = null;
        }

        if (callback != null) {
            callback.run();
        }
    }

    private void recycleResources() {

        ByteBuffer buf = buffer;
        if (buf != null) {
            buffer = null;
            session.getSource().recycle(buffer);

        }
    }

    public void execute() throws Exception {
        startTime = System.currentTimeMillis();
        ServerConnection sc = session.getSource();
        this.isRunning = true;
        this.packetId = 0;
        final BackendConnection conn = session.getTarget(node);

        LOGGER.debug("rrs.getRunOnSlave() " + rrs.getRunOnSlave());
        node.setRunOnSlave(rrs.getRunOnSlave());    // 实现 master/slave注解
        LOGGER.debug("node.getRunOnSlave() " + node.getRunOnSlave());

        if (session.tryExistsCon(conn, node)) {
            _execute(conn);
        } else {
            // create new connection
            SqlEngineConfig conf = SqlEngineServer.getInstance().getConfig();

            LOGGER.debug("node.getRunOnSlave() " + node.getRunOnSlave());
            node.setRunOnSlave(rrs.getRunOnSlave());    // 实现 master/slave注解
            LOGGER.debug("node.getRunOnSlave() " + node.getRunOnSlave());

            PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
            dn.getConnection(dn.getDatabase(), sc.isAutocommit(), node, this, node);
        }

    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        session.bindConnection(node, conn);
        _execute(conn);

    }

    private void _execute(BackendConnection conn) {
        if (session.closed()) {
            endRunning();
            session.clearResources(true);
            return;
        }
        conn.setResponseHandler(this);
        try {
            conn.execute(node, session.getSource(), session.getSource()
                    .isAutocommit());
        } catch (Exception e1) {
            executeException(conn, e1);
            return;
        }
    }

    private void executeException(BackendConnection c, Exception e) {
        ErrorPacket err = new ErrorPacket();
        err.packetId = ++packetId;
        err.errno = ErrorCode.ERR_FOUND_EXCEPION;
        err.message = StringUtil.encode(e.toString(), session.getSource().getCharset());

        this.backConnectionErr(err, c);
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {

        endRunning();
        ErrorPacket err = new ErrorPacket();
        err.packetId = ++packetId;
        err.errno = ErrorCode.ER_NEW_ABORTING_CONNECTION;
        err.message = StringUtil.encode(e.getMessage(), session.getSource().getCharset());

        ServerConnection source = session.getSource();
        source.write(err.write(allocBuffer(), source, true));
    }

    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        ErrorPacket err = new ErrorPacket();
        err.read(data);
        err.packetId = ++packetId;
        backConnectionErr(err, conn);
    }

    private void backConnectionErr(ErrorPacket errPkg, BackendConnection conn) {
        endRunning();

        ServerConnection source = session.getSource();
        String errUser = source.getUser();
        String errHost = source.getHost();
        int errPort = source.getLocalPort();

        String errmgs = " errno:" + errPkg.errno + " " + new String(errPkg.message);
        LOGGER.warn("execute  sql err :" + errmgs + " con:" + conn
                + " frontend host:" + errHost + "/" + errPort + "/" + errUser);

        session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(), false);

        source.setTxInterrupt(errmgs);
        errPkg.write(source);
        recycleResources();
    }

    /**
     * insert/update/delete
     * <p/>
     * okResponse()：读取data字节数组，组成一个OKPacket，并调用ok.write(source)将结果写入前端连接FrontendConnection的写缓冲队列writeQueue中，
     * 真正发送给应用是由对应的NIOSocketWR从写队列中读取ByteBuffer并返回的
     */
    @Override
    public void okResponse(byte[] data, BackendConnection conn) {
        //
        this.netOutBytes += data.length;

        boolean executeResponse = conn.syncAndExcute();
        if (executeResponse) {
            ServerConnection source = session.getSource();
            OkPacket ok = new OkPacket();
            ok.read(data);
            boolean isCanClose2Client =
                    (!rrs.isCallStatement()) || (rrs.isCallStatement() && !rrs.getProcedure().isResultSimpleValue());
            if (isCanClose2Client) {
                ok.packetId = ++packetId;// OK_PACKET
            }

            if (isCanClose2Client) {
                session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(), false);
                endRunning();
            }
            ok.serverStatus = source.isAutocommit() ? 2 : 1;
            recycleResources();

            if (isCanClose2Client) {
                source.setLastInsertId(ok.insertId);
                ok.write(source);
            }

            this.affectedRows = ok.affectedRows;

        }
    }

    /**
     * select
     * <p/>
     * 行结束标志返回时触发，将EOF标志写入缓冲区，最后调用source.write(buffer)将缓冲区放入前端连接的写缓冲队列中，等待NIOSocketWR将其发送给应用
     */
    @Override
    public void rowEofResponse(byte[] eof, BackendConnection conn) {

        this.netOutBytes += eof.length;

        ServerConnection source = session.getSource();
        conn.recordSql(source.getHost(), source.getSchema(), node.getStatement());
        // 判断是调用存储过程的话不能在这里释放链接
        if (!rrs.isCallStatement() || (rrs.isCallStatement() && rrs.getProcedure().isResultSimpleValue())) {
            session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(), false);
            endRunning();
        }

        eof[3] = ++packetId;
        buffer = source.writeToBuffer(eof, allocBuffer());
        int resultSize = source.getWriteQueue().size() * SqlEngineServer.getInstance().getConfig().getSystem()
                .getBufferPoolPageSize();
        resultSize = resultSize + buffer.position();
        source.write(buffer);
        //TODO:
        //查询结果派发
        QueryResult queryResult = new QueryResult(session.getSource().getUser(),
                rrs.getSqlType(), rrs.getStatement(), affectedRows, netInBytes, netOutBytes, startTime,
                System.currentTimeMillis(), resultSize);
        QueryResultDispatcher.dispatchQuery(queryResult);

    }

    /**
     * lazy create ByteBuffer only when needed
     *
     * @return
     */
    private ByteBuffer allocBuffer() {
        if (buffer == null) {
            buffer = session.getSource().allocate();
        }
        return buffer;
    }

    /**
     * select
     * <p/>
     * 元数据返回时触发，将header和元数据内容依次写入缓冲区中
     */
    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {

        this.netOutBytes += header.length;
        for (int i = 0, len = fields.size(); i < len; ++i) {
            byte[] field = fields.get(i);
            this.netOutBytes += field.length;
        }

        header[3] = ++packetId;
        ServerConnection source = session.getSource();
        buffer = source.writeToBuffer(header, allocBuffer());
        for (int i = 0, len = fields.size(); i < len; ++i) {
            byte[] field = fields.get(i);
            field[3] = ++packetId;

            // 保存field信息
            FieldPacket fieldPk = new FieldPacket();
            fieldPk.read(field);
            fieldPackets.add(fieldPk);

            buffer = source.writeToBuffer(field, buffer);
        }

        fieldCount = fieldPackets.size();

        eof[3] = ++packetId;
        buffer = source.writeToBuffer(eof, buffer);

        if (isDefaultNodeShowTable) {

            for (String name : shardingTablesSet) {
                RowDataPacket row = new RowDataPacket(1);
                row.add(StringUtil.encode(name.toLowerCase(), source.getCharset()));
                row.packetId = ++packetId;
                buffer = row.write(buffer, source, true);
            }

        } else if (isDefaultNodeShowFullTable) {

            for (String name : shardingTablesSet) {
                RowDataPacket row = new RowDataPacket(1);
                row.add(StringUtil.encode(name.toLowerCase(), source.getCharset()));
                row.add(StringUtil.encode("BASE TABLE", source.getCharset()));
                row.packetId = ++packetId;
                buffer = row.write(buffer, source, true);
            }
        }
    }

    /**
     * select
     * <p/>
     * 行数据返回时触发，将行数据写入缓冲区中
     */
    @Override
    public void rowResponse(byte[] row, BackendConnection conn) {

        this.netOutBytes += row.length;
        this.selectRows++;

        if (isDefaultNodeShowTable || isDefaultNodeShowFullTable) {
            RowDataPacket rowDataPacket = new RowDataPacket(1);
            rowDataPacket.read(row);
            String table = StringUtil.decode(rowDataPacket.fieldValues.get(0), conn.getCharset());
            if (shardingTablesSet.contains(table.toUpperCase())) {
                return;
            }
        }
        row[3] = ++packetId;

        if (prepared) {
            RowDataPacket rowDataPk = new RowDataPacket(fieldCount);
            rowDataPk.read(row);
            BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
            binRowDataPk.read(fieldPackets, rowDataPk);
            binRowDataPk.packetId = rowDataPk.packetId;
            //			binRowDataPk.write(session.getSource());
            /*
             * [fix bug] : 这里不能直接将包写到前端连接,
			 * 因为在fieldEofResponse()方法结束后buffer还没写出,
			 * 所以这里应该将包数据顺序写入buffer(如果buffer满了就写出),然后再将buffer写出
			 */
            buffer = binRowDataPk.write(buffer, session.getSource(), true);
        } else {
            buffer = session.getSource().writeToBuffer(row, allocBuffer());
            //session.getSource().write(row);
        }

    }

    @Override
    public void writeQueueAvailable() {

    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        ErrorPacket err = new ErrorPacket();
        err.packetId = ++packetId;
        err.errno = ErrorCode.ER_ERROR_ON_CLOSE;
        err.message = StringUtil.encode(reason, session.getSource()
                .getCharset());
        this.backConnectionErr(err, conn);

    }

    public void clearResources() {

    }

    public boolean isPrepared() {
        return prepared;
    }

    public void setPrepared(boolean prepared) {
        this.prepared = prepared;
    }

    @Override
    public String toString() {
        return "SingleNodeHandler [node=" + node + ", packetId=" + packetId + "]";
    }

}