package com.baidu.sqlengine.backend.mysql.nio.handler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.backend.BackendConnection;
import com.baidu.sqlengine.backend.datasource.PhysicalDBNode;
import com.baidu.sqlengine.backend.mysql.protocol.ErrorPacket;
import com.baidu.sqlengine.backend.mysql.protocol.RowDataPacket;
import com.baidu.sqlengine.cache.CachePool;
import com.baidu.sqlengine.config.SqlEngineConfig;
import com.baidu.sqlengine.route.RouteResultsetNode;
import com.baidu.sqlengine.server.parser.ServerParse;

/**
 * company where id=(select company_id from customer where id=3); the one which
 * return data (id) is the datanode to store child table's records
 */
public class FetchStoreNodeOfChildTableHandler implements ResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(FetchStoreNodeOfChildTableHandler.class);
    private String sql;
    private volatile String result;
    private volatile String dataNode;
    private AtomicInteger finished = new AtomicInteger(0);
    protected final ReentrantLock lock = new ReentrantLock();

    public String execute(String schema, String sql, ArrayList<String> dataNodes) {
        String key = schema + ":" + sql;
        CachePool cache = SqlEngineServer.getInstance().getCacheService().getCachePool("ER_SQL2PARENTID");
        String result = (String) cache.get(key);
        if (result != null) {
            return result;
        }
        this.sql = sql;
        int totalCount = dataNodes.size();
        long startTime = System.currentTimeMillis();
        long endTime = startTime + 5 * 60 * 1000L;
        SqlEngineConfig conf = SqlEngineServer.getInstance().getConfig();

        LOGGER.debug("find child node with sql:" + sql);
        for (String dn : dataNodes) {
            if (dataNode != null) {
                return dataNode;
            }
            PhysicalDBNode mysqlDN = conf.getDataNodes().get(dn);
            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("execute in datanode " + dn);
                }
                RouteResultsetNode node = new RouteResultsetNode(dn, ServerParse.SELECT, sql);
                node.setRunOnSlave(false);    // 获取 子表节点，最好走master为好

                mysqlDN.getConnection(mysqlDN.getDatabase(), true, node, this, dn);

            } catch (Exception e) {
                LOGGER.warn("get connection err " + e);
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {

            }
        }

        while (dataNode == null && System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                break;
            }
            if (dataNode != null || finished.get() >= totalCount) {
                break;
            }
        }
        if (dataNode != null) {
            cache.putIfAbsent(key, dataNode);
        }
        return dataNode;

    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        conn.setResponseHandler(this);
        try {
            conn.query(sql);
        } catch (Exception e) {
            executeException(conn, e);
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        finished.incrementAndGet();
        LOGGER.warn("connectionError " + e);

    }

    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        finished.incrementAndGet();
        ErrorPacket err = new ErrorPacket();
        err.read(data);
        LOGGER.warn("errorResponse " + err.errno + " "
                + new String(err.message));
        conn.release();

    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        boolean executeResponse = conn.syncAndExcute();
        if (executeResponse) {
            finished.incrementAndGet();
            conn.release();
        }

    }

    @Override
    public void rowResponse(byte[] row, BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received rowResponse response," + getColumn(row)
                    + " from  " + conn);
        }
        if (result == null) {
            result = getColumn(row);
            dataNode = (String) conn.getAttachment();
        } else {
            LOGGER.warn("find multi data nodes for child table store, sql is:  "
                    + sql);
        }

    }

    private String getColumn(byte[] row) {
        RowDataPacket rowDataPkg = new RowDataPacket(1);
        rowDataPkg.read(row);
        byte[] columnData = rowDataPkg.fieldValues.get(0);
        return new String(columnData);
    }

    @Override
    public void rowEofResponse(byte[] eof, BackendConnection conn) {
        finished.incrementAndGet();
        conn.release();
    }

    private void executeException(BackendConnection c, Throwable e) {
        finished.incrementAndGet();
        LOGGER.warn("executeException   " + e);
        c.close("exception:" + e);

    }

    @Override
    public void writeQueueAvailable() {

    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {

        LOGGER.warn("connection closed " + conn + " reason:" + reason);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {

    }

}