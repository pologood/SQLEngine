package com.baidu.sqlengine.backend.datasource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.backend.BackendConnection;
import com.baidu.sqlengine.backend.ConnectionMap;
import com.baidu.sqlengine.backend.ConnectionQueue;
import com.baidu.sqlengine.backend.heartbeat.DBHeartbeat;
import com.baidu.sqlengine.backend.mysql.nio.MySQLConnection;
import com.baidu.sqlengine.backend.mysql.nio.handler.ConnectionHeartBeatHandler;
import com.baidu.sqlengine.backend.mysql.nio.handler.DelegateResponseHandler;
import com.baidu.sqlengine.backend.mysql.nio.handler.NewConnectionRespHandler;
import com.baidu.sqlengine.backend.mysql.nio.handler.ResponseHandler;
import com.baidu.sqlengine.config.model.DBHostConfig;
import com.baidu.sqlengine.config.model.DataHostConfig;
import com.baidu.sqlengine.constant.Alarms;
import com.baidu.sqlengine.util.TimeUtil;

public abstract class PhysicalDatasource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDatasource.class);

    private final String name;
    private final int size;
    private final DBHostConfig config;
    private final ConnectionMap conMap = new ConnectionMap();
    private DBHeartbeat heartbeat;
    private final boolean readNode;
    private volatile long heartbeatRecoveryTime;
    private final DataHostConfig hostConfig;
    private final ConnectionHeartBeatHandler conHeartBeatHanler = new ConnectionHeartBeatHandler();
    private PhysicalDBPool dbPool;

    // 添加DataSource读计数
    private AtomicLong readCount = new AtomicLong(0);

    // 添加DataSource写计数
    private AtomicLong writeCount = new AtomicLong(0);

    public PhysicalDatasource(DBHostConfig config, DataHostConfig hostConfig, boolean isReadNode) {
        this.size = config.getMaxCon();
        this.config = config;
        this.name = config.getHostName();
        this.hostConfig = hostConfig;
        heartbeat = this.createHeartBeat();
        this.readNode = isReadNode;
    }

    public boolean isMyConnection(BackendConnection con) {
        if (con instanceof MySQLConnection) {
            return ((MySQLConnection) con).getPool() == this;
        } else {
            return false;
        }

    }

    public long getReadCount() {
        return readCount.get();
    }

    public void setReadCount() {
        readCount.addAndGet(1);
    }

    public long getWriteCount() {
        return writeCount.get();
    }

    public void setWriteCount() {
        writeCount.addAndGet(1);
    }

    public DataHostConfig getHostConfig() {
        return hostConfig;
    }

    public boolean isReadNode() {
        return readNode;
    }

    public int getSize() {
        return size;
    }

    public void setDbPool(PhysicalDBPool dbPool) {
        this.dbPool = dbPool;
    }

    public PhysicalDBPool getDbPool() {
        return dbPool;
    }

    public abstract DBHeartbeat createHeartBeat();

    public String getName() {
        return name;
    }

    public long getExecuteCount() {
        long executeCount = 0;
        for (ConnectionQueue queue : conMap.getAllConQueue()) {
            executeCount += queue.getExecuteCount();

        }
        return executeCount;
    }

    public long getExecuteCountForSchema(String schema) {
        return conMap.getSchemaConQueue(schema).getExecuteCount();

    }

    public int getActiveCountForSchema(String schema) {
        return conMap.getActiveCountForSchema(schema, this);
    }

    public int getIdleCountForSchema(String schema) {
        ConnectionQueue queue = conMap.getSchemaConQueue(schema);
        int total = 0;
        total += queue.getAutoCommitCons().size() + queue.getManCommitCons().size();
        return total;
    }

    public DBHeartbeat getHeartbeat() {
        return heartbeat;
    }

    public int getIdleCount() {
        int total = 0;
        for (ConnectionQueue queue : conMap.getAllConQueue()) {
            total += queue.getAutoCommitCons().size() + queue.getManCommitCons().size();
        }
        return total;
    }

    private boolean validSchema(String schema) {
        String theSchema = schema;
        return theSchema != null && !"".equals(theSchema) && !"snyn...".equals(theSchema);
    }

    private void checkIfNeedHeartBeat(LinkedList<BackendConnection> heartBeatCons, ConnectionQueue queue,
                                      ConcurrentLinkedQueue<BackendConnection> checkLis, long hearBeatTime,
                                      long hearBeatTime2) {
        int maxConsInOneCheck = 10;
        Iterator<BackendConnection> checkListItor = checkLis.iterator();
        while (checkListItor.hasNext()) {
            BackendConnection con = checkListItor.next();
            if (con.isClosedOrQuit()) {
                checkListItor.remove();
                continue;
            }
            if (validSchema(con.getSchema())) {
                if (con.getLastTime() < hearBeatTime && heartBeatCons.size() < maxConsInOneCheck) {
                    checkListItor.remove();
                    // Heart beat check
                    con.setBorrowed(true);
                    heartBeatCons.add(con);
                }
            } else if (con.getLastTime() < hearBeatTime2) {
                // not valid schema conntion should close for idle
                // exceed 2*conHeartBeatPeriod
                checkListItor.remove();
                con.close(" heart beate idle ");
            }

        }

    }

    public int getIndex() {
        int currentIndex = 0;
        for (int i = 0; i < dbPool.getSources().length; i++) {
            PhysicalDatasource writeHostDatasource = dbPool.getSources()[i];
            if (writeHostDatasource.getName().equals(getName())) {
                currentIndex = i;
                break;
            }
        }
        return currentIndex;
    }

    public boolean isSalveOrRead() {
        int currentIndex = getIndex();
        if (currentIndex != dbPool.activedIndex || this.readNode) {
            return true;
        }
        return false;
    }

    public void heatBeatCheck(long timeout, long conHeartBeatPeriod) {
        int maxConsInOneCheck = 5;
        LinkedList<BackendConnection> heartBeatCons = new LinkedList<BackendConnection>();

        long hearBeatTime = TimeUtil.currentTimeMillis() - conHeartBeatPeriod;
        long hearBeatTime2 = TimeUtil.currentTimeMillis() - 2 * conHeartBeatPeriod;
        for (ConnectionQueue queue : conMap.getAllConQueue()) {
            checkIfNeedHeartBeat(heartBeatCons, queue, queue.getAutoCommitCons(), hearBeatTime, hearBeatTime2);
            if (heartBeatCons.size() < maxConsInOneCheck) {
                checkIfNeedHeartBeat(heartBeatCons, queue, queue.getManCommitCons(), hearBeatTime, hearBeatTime2);
            } else if (heartBeatCons.size() >= maxConsInOneCheck) {
                break;
            }
        }

        if (!heartBeatCons.isEmpty()) {
            for (BackendConnection con : heartBeatCons) {
                conHeartBeatHanler.doHeartBeat(con, hostConfig.getHearbeatSQL());
            }
        }

        // check if there has timeouted heatbeat cons
        conHeartBeatHanler.abandTimeOuttedConns();
        int idleCons = getIdleCount();
        int activeCons = this.getActiveCount();
        int createCount = (hostConfig.getMinCon() - idleCons) / 3;
        // create if idle too little
        if ((createCount > 0) && (idleCons + activeCons < size) && (idleCons < hostConfig.getMinCon())) {
            createByIdleLitte(idleCons, createCount);
        } else if (idleCons > hostConfig.getMinCon()) {
            closeByIdleMany(idleCons - hostConfig.getMinCon());
        } else {
            int activeCount = this.getActiveCount();
            if (activeCount > size) {
                StringBuilder s = new StringBuilder();
                s.append(Alarms.DEFAULT).append("DATASOURCE EXCEED [name=").append(name).append(",active=");
                s.append(activeCount).append(",size=").append(size).append(']');
                LOGGER.warn(s.toString());
            }
        }
    }

    private void closeByIdleMany(int ildeCloseCount) {
        LOGGER.info("too many ilde cons ,close some for datasouce  " + name);
        List<BackendConnection> readyCloseCons = new ArrayList<BackendConnection>(ildeCloseCount);
        for (ConnectionQueue queue : conMap.getAllConQueue()) {
            readyCloseCons.addAll(queue.getIdleConsToClose(ildeCloseCount));
            if (readyCloseCons.size() >= ildeCloseCount) {
                break;
            }
        }

        for (BackendConnection idleCon : readyCloseCons) {
            if (idleCon.isBorrowed()) {
                LOGGER.warn("find idle con is using " + idleCon);
            }
            idleCon.close("too many idle con");
        }
    }

    private void createByIdleLitte(int idleCons, int createCount) {
        LOGGER.info("create connections ,because idle connection not enough ,cur is "
                + idleCons
                + ", minCon is "
                + hostConfig.getMinCon()
                + " for "
                + name);
        NewConnectionRespHandler simpleHandler = new NewConnectionRespHandler();

        final String[] schemas = dbPool.getSchemas();
        for (int i = 0; i < createCount; i++) {
            if (this.getActiveCount() + this.getIdleCount() >= size) {
                break;
            }
            try {
                // creat new connection
                this.createNewConnection(simpleHandler, null, schemas[i % schemas.length]);
            } catch (IOException e) {
                LOGGER.warn("create connection err " + e);
            }

        }
    }

    public int getActiveCount() {
        return this.conMap.getActiveCountForDs(this);
    }

    public void clearCons(String reason) {
        this.conMap.clearConnections(reason, this);
    }

    public void startHeartbeat() {
        heartbeat.start();
    }

    public void stopHeartbeat() {
        heartbeat.stop();
    }

    public void doHeartbeat() {
        // 未到预定恢复时间，不执行心跳检测。
        if (TimeUtil.currentTimeMillis() < heartbeatRecoveryTime) {
            return;
        }

        if (!heartbeat.isStop()) {
            try {
                heartbeat.heartbeat();
            } catch (Exception e) {
                LOGGER.error(name + " heartbeat error.", e);
            }
        }
    }

    private BackendConnection takeConnection(BackendConnection conn, final ResponseHandler handler,
                                             final Object attachment, String schema) {

        conn.setBorrowed(true);
        if (!conn.getSchema().equals(schema)) {
            // need do schema syn in before sql send
            conn.setSchema(schema);
        }
        ConnectionQueue queue = conMap.getSchemaConQueue(schema);
        queue.incExecuteCount();
        conn.setAttachment(attachment);
        conn.setLastTime(System.currentTimeMillis()); // 每次取连接的时候，更新下lasttime，防止在前端连接检查的时候，关闭连接，导致sql执行失败
        handler.connectionAcquired(conn);
        return conn;
    }

    private void createNewConnection(final ResponseHandler handler, final Object attachment, final String schema)
            throws IOException {
        // aysn create connection
        SqlEngineServer.getInstance().getBusinessExecutor().execute(new Runnable() {
            public void run() {
                try {
                    createNewConnection(new DelegateResponseHandler(handler) {
                        @Override
                        public void connectionError(Throwable e, BackendConnection conn) {
                            handler.connectionError(e, conn);
                        }

                        @Override
                        public void connectionAcquired(BackendConnection conn) {
                            takeConnection(conn, handler, attachment, schema);
                        }
                    }, schema);
                } catch (IOException e) {
                    handler.connectionError(e, null);
                }
            }
        });
    }

    public void getConnection(String schema, boolean autocommit, final ResponseHandler handler, final Object attachment)
            throws IOException {

        // 从当前连接map中拿取已建立好的后端连接
        BackendConnection con = this.conMap.tryTakeConnection(schema, autocommit);
        if (con != null) {
            //如果不为空，则绑定对应前端请求的handler
            takeConnection(con, handler, attachment, schema);
            return;

        } else {
            int activeCons = this.getActiveCount();// 当前最大活动连接
            if (activeCons + 1 > size) {// 下一个连接大于最大连接数
                LOGGER.error("the max activeConnnections size can not be max than maxconnections");
                throw new IOException("the max activeConnnections size can not be max than maxconnections");
            } else { // create connection
                LOGGER.info(
                        "no ilde connection in pool,create new connection for " + this.name + " of schema " + schema);
                createNewConnection(handler, attachment, schema);
            }
        }
    }

    private void returnConnection(BackendConnection c) {
        c.setAttachment(null);
        c.setBorrowed(false);
        c.setLastTime(TimeUtil.currentTimeMillis());
        ConnectionQueue queue = this.conMap.getSchemaConQueue(c.getSchema());

        boolean ok = false;
        if (c.isAutocommit()) {
            ok = queue.getAutoCommitCons().offer(c);
        } else {
            ok = queue.getManCommitCons().offer(c);
        }

        if (!ok) {
            LOGGER.warn("can't return to pool ,so close con " + c);
            c.close("can't return to pool ");
        }
    }

    public void releaseChannel(BackendConnection c) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("release channel " + c);
        }
        // release connection
        returnConnection(c);
    }

    public void connectionClosed(BackendConnection conn) {
        ConnectionQueue queue = this.conMap.getSchemaConQueue(conn.getSchema());
        if (queue != null) {
            queue.removeCon(conn);
        }

    }

    /**
     * 创建新连接
     */
    public abstract void createNewConnection(ResponseHandler handler, String schema) throws IOException;

    /**
     * 测试连接，用于初始化及热更新配置检测
     */
    public abstract boolean testConnection(String schema) throws IOException;

    public long getHeartbeatRecoveryTime() {
        return heartbeatRecoveryTime;
    }

    public void setHeartbeatRecoveryTime(long heartbeatRecoveryTime) {
        this.heartbeatRecoveryTime = heartbeatRecoveryTime;
    }

    public DBHostConfig getConfig() {
        return config;
    }
}
