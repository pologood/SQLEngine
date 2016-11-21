package com.baidu.sqlengine.server;

import java.io.IOException;
import java.nio.channels.NetworkChannel;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.route.RouteResultSet;
import com.baidu.sqlengine.server.response.InformationSchemaProfiling;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.net.FrontendConnection;
import com.baidu.sqlengine.server.handler.MysqlInformationSchemaHandler;
import com.baidu.sqlengine.server.handler.MysqlProcHandler;
import com.baidu.sqlengine.server.parser.ServerParse;
import com.baidu.sqlengine.server.response.Heartbeat;
import com.baidu.sqlengine.server.response.Ping;
import com.baidu.sqlengine.server.util.SchemaUtil;
import com.baidu.sqlengine.util.TimeUtil;

public class ServerConnection extends FrontendConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerConnection.class);
    private static final long AUTH_TIMEOUT = 15 * 1000L;

    private volatile int txIsolation;
    private volatile boolean autocommit;
    private volatile boolean txInterrupted;
    private volatile String txInterrputMsg = "";
    private long lastInsertId;
    private NonBlockingSession session;

    public ServerConnection(NetworkChannel channel) throws IOException {
        super(channel);
        this.txInterrupted = false;
        this.autocommit = true;
    }

    @Override
    public boolean isIdleTimeout() {
        if (isAuthenticated) {
            return super.isIdleTimeout();
        } else {
            return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + AUTH_TIMEOUT;
        }
    }

    public int getTxIsolation() {
        return txIsolation;
    }

    public void setTxIsolation(int txIsolation) {
        this.txIsolation = txIsolation;
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
    }

    public long getLastInsertId() {
        return lastInsertId;
    }

    public void setLastInsertId(long lastInsertId) {
        this.lastInsertId = lastInsertId;
    }

    /**
     * 设置是否需要中断当前事务
     */
    public void setTxInterrupt(String txInterrputMsg) {
        if (!autocommit && !txInterrupted) {
            txInterrupted = true;
            this.txInterrputMsg = txInterrputMsg;
        }
    }

    public boolean isTxInterrupted() {
        return txInterrupted;
    }

    public NonBlockingSession getSession() {
        return session;
    }

    public void setSession(NonBlockingSession session) {
        this.session = session;
    }

    @Override
    public void ping() {
        Ping.response(this);
    }

    @Override
    public void heartbeat(byte[] data) {
        Heartbeat.response(this, data);
    }

    public void execute(String sql, int type) {
        //连接状态检查
        if (this.isClosed()) {
            LOGGER.warn("ignore execute ,server connection is closed " + this);
            return;
        }
        // 事务状态检查
        if (txInterrupted) {
            writeErrMessage(ErrorCode.ER_YES,
                    "Transaction error, need to rollback." + txInterrputMsg);
            return;
        }

        // 检查当前使用的DB
        String db = this.schema;
        boolean isDefault = true;
        if (db == null) {
            db = SchemaUtil.detectDefaultDb(sql, type);
            if (db == null) {
                writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "No SqlEngine Database selected");
                return;
            }
            isDefault = false;
        }

        // 兼容PhpAdmin's, 支持对MySQL元数据的模拟返回
        //// TODO:支持更多information_schema特性
        if (ServerParse.SELECT == type && db.equalsIgnoreCase("information_schema")) {
            MysqlInformationSchemaHandler.handle(sql, this);
            return;
        }

        if (ServerParse.SELECT == type
                && sql.contains("mysql")
                && sql.contains("proc")) {

            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
            if (schemaInfo != null
                    && "mysql".equalsIgnoreCase(schemaInfo.schema)
                    && "proc".equalsIgnoreCase(schemaInfo.table)) {

                // 兼容MySQLWorkbench
                MysqlProcHandler.handle(sql, this);
                return;
            }
        }

        SchemaConfig schema = SqlEngineServer.getInstance().getConfig().getSchemas().get(db);
        if (schema == null) {
            writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "Unknown SqlEngine Database '" + db + "'");
            return;
        }

        //fix navicat   SELECT STATE AS `State`, ROUND(SUM(DURATION),7) AS `Duration`, CONCAT(ROUND(SUM(DURATION)
        // /*100,3), '%') AS `Percentage` FROM INFORMATION_SCHEMA.PROFILING WHERE QUERY_ID= GROUP BY STATE ORDER BY SEQ
        if (ServerParse.SELECT == type && sql.contains(" INFORMATION_SCHEMA.PROFILING ") && sql
                .contains("CONCAT(ROUND(SUM(DURATION)/*100,3)")) {
            InformationSchemaProfiling.response(this);
            return;
        }

		/* 当已经设置默认schema时，可以通过在sql中指定其它schema的方式执行
         * 相关sql，已经在mysql客户端中验证。
		 * 所以在此处增加关于sql中指定Schema方式的支持。
		 */
        if (isDefault && schema.isCheckSQLSchema() && isNormalSql(type)) {
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
            if (schemaInfo != null && schemaInfo.schema != null && !schemaInfo.schema.equals(db)) {
                SchemaConfig schemaConfig =
                        SqlEngineServer.getInstance().getConfig().getSchemas().get(schemaInfo.schema);
                if (schemaConfig != null) {
                    schema = schemaConfig;
                }
            }
        }

        routeEndExecuteSQL(sql, type, schema);

    }

    private boolean isNormalSql(int type) {
        return ServerParse.SELECT == type || ServerParse.INSERT == type || ServerParse.UPDATE == type
                || ServerParse.DELETE == type || ServerParse.DDL == type;
    }

    public RouteResultSet routeSQL(String sql, int type) {

        // 检查当前使用的DB
        String db = this.schema;
        if (db == null) {
            writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "No SqlEngine Database selected");
            return null;
        }
        SchemaConfig schema = SqlEngineServer.getInstance().getConfig().getSchemas().get(db);
        if (schema == null) {
            writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "Unknown SqlEngine Database '" + db + "'");
            return null;
        }

        // 路由计算
        RouteResultSet rrs = null;
        try {
            rrs = SqlEngineServer
                    .getInstance()
                    .getRouterservice()
                    .route(SqlEngineServer.getInstance().getConfig().getSystem(), schema, type, sql, this.charset,
                            this);

        } catch (Exception e) {
            StringBuilder s = new StringBuilder();
            LOGGER.warn(s.append(this).append(sql).toString() + " err:" + e.toString(), e);
            String msg = e.getMessage();
            writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
            return null;
        }
        return rrs;
    }

    public void routeEndExecuteSQL(String sql, int type, SchemaConfig schema) {
        // 路由计算
        RouteResultSet rrs = null;
        try {
            rrs = SqlEngineServer
                    .getInstance()
                    .getRouterservice()
                    .route(SqlEngineServer.getInstance().getConfig().getSystem(), schema, type, sql, this.charset,
                            this);

        } catch (Exception e) {
            StringBuilder s = new StringBuilder();
            LOGGER.warn(s.append(this).append(sql).toString() + " err:" + e.toString(), e);
            String msg = e.getMessage();
            writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
            return;
        }
        if (rrs != null) {
            // session执行
            session.execute(rrs, type);
        }
    }

    /**
     * 提交事务
     */
    public void commit() {
        if (txInterrupted) {
            writeErrMessage(ErrorCode.ER_YES, "Transaction error, need to rollback.");
        } else {
            session.commit();
        }
    }

    /**
     * 回滚事务
     */
    public void rollback() {
        // 状态检查
        if (txInterrupted) {
            txInterrupted = false;
        }

        // 执行回滚
        session.rollback();
    }

    /**
     * 撤销执行中的语句
     *
     * @param sponsor 发起者为null表示是自己
     */
    public void cancel(final FrontendConnection sponsor) {
        processor.getExecutor().execute(new Runnable() {
            @Override
            public void run() {
                session.cancel(sponsor);
            }
        });
    }

    @Override
    public void close(String reason) {
        super.close(reason);
        session.terminate();
    }

    @Override
    public String toString() {
        return "ServerConnection [id=" + id + ", schema=" + schema + ", host="
                + host + ", user=" + user + ",txIsolation=" + txIsolation
                + ", autocommit=" + autocommit + ", schema=" + schema + "]";
    }

}
