package com.baidu.sqlengine.manager.response;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.SqlEngineServer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.baidu.sqlengine.backend.BackendConnection;
import com.baidu.sqlengine.backend.datasource.PhysicalDBNode;
import com.baidu.sqlengine.backend.datasource.PhysicalDBPool;
import com.baidu.sqlengine.backend.datasource.PhysicalDatasource;
import com.baidu.sqlengine.backend.jdbc.JDBCConnection;
import com.baidu.sqlengine.backend.mysql.nio.MySQLConnection;
import com.baidu.sqlengine.config.ConfigInitializer;
import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.config.SqlEngineCluster;
import com.baidu.sqlengine.config.SqlEngineConfig;
import com.baidu.sqlengine.config.model.FirewallConfig;
import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.config.model.UserConfig;
import com.baidu.sqlengine.config.util.DnPropertyUtil;
import com.baidu.sqlengine.manager.ManagerConnection;
import com.baidu.sqlengine.net.NIOProcessor;
import com.baidu.sqlengine.backend.mysql.protocol.OkPacket;

public final class ReloadConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadConfig.class);

    public static void execute(ManagerConnection c, final boolean loadAll) {
        final ReentrantLock lock = SqlEngineServer.getInstance().getConfig().getLock();
        lock.lock();
        try {
            ListenableFuture<Boolean> listenableFuture =
                    SqlEngineServer.getInstance().getListeningExecutorService().submit(
                            new Callable<Boolean>() {
                                @Override
                                public Boolean call() throws Exception {
                                    return loadAll ? reloadAll() : reload();
                                }
                            }
                    );
            Futures.addCallback(listenableFuture, new ReloadCallBack(c),
                    SqlEngineServer.getInstance().getListeningExecutorService());
        } finally {
            lock.unlock();
        }
    }

    private static boolean reloadAll() {

        /**
         *  1、载入新的配置
         *  1.1、ConfigInitializer 初始化，基本自检
         *  1.2、DataNode/DataHost 实际链路检测
         */
        ConfigInitializer loader = new ConfigInitializer(true);
        Map<String, UserConfig> newUsers = loader.getUsers();
        Map<String, SchemaConfig> newSchemas = loader.getSchemas();
        Map<String, PhysicalDBNode> newDataNodes = loader.getDataNodes();
        Map<String, PhysicalDBPool> newDataHosts = loader.getDataHosts();
        SqlEngineCluster newCluster = loader.getCluster();
        FirewallConfig newFirewall = loader.getFirewall();

        /**
         * 1.2、实际链路检测
         */
        loader.testConnection();

        /**
         *  2、承接
         *  2.1、老的 dataSource 继续承接新建请求
         *  2.2、新的 dataSource 开始初始化， 完毕后交由 2.3
         *  2.3、新的 dataSource 开始承接新建请求
         *  2.4、老的 dataSource 内部的事务执行完毕， 相继关闭
         *  2.5、老的 dataSource 超过阀值的，强制关闭
         */

        SqlEngineConfig config = SqlEngineServer.getInstance().getConfig();

        /**
         * 2.1 、老的 dataSource 继续承接新建请求， 此处什么也不需要做
         */

        boolean isReloadStatusOK = true;

        /**
         * 2.2、新的 dataHosts 初始化
         */
        for (PhysicalDBPool dbPool : newDataHosts.values()) {
            String hostName = dbPool.getHostName();

            // 设置 schemas
            ArrayList<String> dnSchemas = new ArrayList<String>(30);
            for (PhysicalDBNode dn : newDataNodes.values()) {
                if (dn.getDbPool().getHostName().equals(hostName)) {
                    dnSchemas.add(dn.getDatabase());
                }
            }
            dbPool.setSchemas(dnSchemas.toArray(new String[dnSchemas.size()]));

            // 获取 data host
            String dnIndex = DnPropertyUtil.loadDnIndexProps().getProperty(dbPool.getHostName(), "0");
            if (!"0".equals(dnIndex)) {
                LOGGER.info("init datahost: " + dbPool.getHostName() + "  to use datasource index:" + dnIndex);
            }

            dbPool.init(Integer.valueOf(dnIndex));
            if (!dbPool.isInitSuccess()) {
                isReloadStatusOK = false;
                break;
            }
        }

        /**
         *  TODO： 确认初始化情况
         *
         *  新的 dataHosts 是否初始化成功
         */
        if (isReloadStatusOK) {

            /**
             * 2.3、 在老的配置上，应用新的配置，开始准备承接任务
             */
            config.reload(newUsers, newSchemas, newDataNodes, newDataHosts, newCluster, newFirewall, true);

            /**
             * 2.4、 处理旧的资源
             */
            LOGGER.warn("1、clear old backend connection(size): " + NIOProcessor.backends_old.size());

            // 清除前一次 reload 转移出去的 old Cons
            Iterator<BackendConnection> iter = NIOProcessor.backends_old.iterator();
            while (iter.hasNext()) {
                BackendConnection con = iter.next();
                con.close("clear old datasources");
                iter.remove();
            }

            Map<String, PhysicalDBPool> oldDataHosts = config.getBackupDataHosts();
            for (PhysicalDBPool dbPool : oldDataHosts.values()) {
                dbPool.stopHeartbeat();

                // 提取数据源下的所有连接
                for (PhysicalDatasource ds : dbPool.getAllDataSources()) {
                    //
                    for (NIOProcessor processor : SqlEngineServer.getInstance().getProcessors()) {
                        for (BackendConnection con : processor.getBackends().values()) {
                            if (con instanceof MySQLConnection) {
                                MySQLConnection mysqlCon = (MySQLConnection) con;
                                if (mysqlCon.getPool() == ds) {
                                    NIOProcessor.backends_old.add(con);
                                }

                            } else if (con instanceof JDBCConnection) {
                                JDBCConnection jdbcCon = (JDBCConnection) con;
                                if (jdbcCon.getPool() == ds) {
                                    NIOProcessor.backends_old.add(con);
                                }
                            }
                        }
                    }
                }
            }
            LOGGER.warn("2、to be recycled old backend connection(size): " + NIOProcessor.backends_old.size());

            //清理缓存
            SqlEngineServer.getInstance().getCacheService().clearCache();
            return true;

        } else {
            // 如果重载不成功，则清理已初始化的资源。
            LOGGER.warn("reload failed, clear previously created datasources ");
            for (PhysicalDBPool dbPool : newDataHosts.values()) {
                dbPool.clearDataSources("reload config");
                dbPool.stopHeartbeat();
            }
            return false;
        }
    }

    private static boolean reload() {

        /**
         *  1、载入新的配置， ConfigInitializer 内部完成自检工作, 由于不更新数据源信息,此处不自检 dataHost  dataNode
         */
        ConfigInitializer loader = new ConfigInitializer(false);
        Map<String, UserConfig> users = loader.getUsers();
        Map<String, SchemaConfig> schemas = loader.getSchemas();
        Map<String, PhysicalDBNode> dataNodes = loader.getDataNodes();
        Map<String, PhysicalDBPool> dataHosts = loader.getDataHosts();
        SqlEngineCluster cluster = loader.getCluster();
        FirewallConfig firewall = loader.getFirewall();

        /**
         * 2、在老的配置上，应用新的配置
         */
        SqlEngineServer.getInstance().getConfig()
                .reload(users, schemas, dataNodes, dataHosts, cluster, firewall, false);

        /**
         * 3、清理缓存
         */
        SqlEngineServer.getInstance().getCacheService().clearCache();
        return true;
    }

    /**
     * 异步执行回调类，用于回写数据给用户等。
     */
    private static class ReloadCallBack implements FutureCallback<Boolean> {

        private ManagerConnection mc;

        private ReloadCallBack(ManagerConnection c) {
            this.mc = c;
        }

        @Override
        public void onSuccess(Boolean result) {
            if (result) {
                LOGGER.warn("send ok package to client " + String.valueOf(mc));
                OkPacket ok = new OkPacket();
                ok.packetId = 1;
                ok.affectedRows = 1;
                ok.serverStatus = 2;
                ok.message = "Reload config success".getBytes();
                ok.write(mc);
            } else {
                mc.writeErrMessage(ErrorCode.ER_YES, "Reload config failure");
            }
        }

        @Override
        public void onFailure(Throwable t) {
            mc.writeErrMessage(ErrorCode.ER_YES, "Reload config failure");
        }
    }
}