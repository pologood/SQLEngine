package com.baidu.sqlengine.config;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.NetworkChannel;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import com.baidu.sqlengine.backend.datasource.PhysicalDBNode;
import com.baidu.sqlengine.backend.datasource.PhysicalDBPool;
import com.baidu.sqlengine.config.model.FirewallConfig;
import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.config.model.SystemConfig;
import com.baidu.sqlengine.config.model.UserConfig;
import com.baidu.sqlengine.net.AbstractConnection;
import com.baidu.sqlengine.util.TimeUtil;

public class SqlEngineConfig {

    private static final int RELOAD = 1;
    private static final int ROLLBACK = 2;
    private static final int RELOAD_ALL = 3;

    private volatile SystemConfig system;
    private volatile SqlEngineCluster cluster;
    private volatile SqlEngineCluster _cluster;
    private volatile FirewallConfig firewall;
    private volatile FirewallConfig _firewall;
    private volatile Map<String, UserConfig> users;
    private volatile Map<String, UserConfig> _users;
    private volatile Map<String, SchemaConfig> schemas;
    private volatile Map<String, SchemaConfig> _schemas;
    private volatile Map<String, PhysicalDBNode> dataNodes;
    private volatile Map<String, PhysicalDBNode> _dataNodes;
    private volatile Map<String, PhysicalDBPool> dataHosts;
    private volatile Map<String, PhysicalDBPool> _dataHosts;
    private long reloadTime;
    private long rollbackTime;
    private int status;
    private final ReentrantLock lock;

    public SqlEngineConfig() {

        //读取schema.xml，rule.xml和server.xml
        ConfigInitializer confInit = new ConfigInitializer(true);
        this.system = confInit.getSystem();
        this.users = confInit.getUsers();
        this.schemas = confInit.getSchemas();
        this.dataHosts = confInit.getDataHosts();

        this.dataNodes = confInit.getDataNodes();
        for (PhysicalDBPool dbPool : dataHosts.values()) {
            dbPool.setSchemas(getDataNodeSchemasOfDataHost(dbPool.getHostName()));
        }

        this.firewall = confInit.getFirewall();
        this.cluster = confInit.getCluster();

        //初始化重加载配置时间
        this.reloadTime = TimeUtil.currentTimeMillis();
        this.rollbackTime = -1L;
        this.status = RELOAD;

        //配置加载锁
        this.lock = new ReentrantLock();
    }

    public SystemConfig getSystem() {
        return system;
    }

    public void setSocketParams(AbstractConnection con, boolean isFrontChannel)
            throws IOException {

        int sorcvbuf = 0;
        int sosndbuf = 0;
        int soNoDelay = 0;
        if (isFrontChannel) {
            sorcvbuf = system.getFrontsocketsorcvbuf();
            sosndbuf = system.getFrontsocketsosndbuf();
            soNoDelay = system.getFrontSocketNoDelay();
        } else {
            sorcvbuf = system.getBacksocketsorcvbuf();
            sosndbuf = system.getBacksocketsosndbuf();
            soNoDelay = system.getBackSocketNoDelay();
        }

        NetworkChannel channel = con.getChannel();
        channel.setOption(StandardSocketOptions.SO_RCVBUF, sorcvbuf);
        channel.setOption(StandardSocketOptions.SO_SNDBUF, sosndbuf);
        channel.setOption(StandardSocketOptions.TCP_NODELAY, soNoDelay == 1);
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);

        con.setMaxPacketSize(system.getMaxPacketSize());
        con.setPacketHeaderSize(system.getPacketHeaderSize());
        con.setIdleTimeout(system.getIdleTimeout());
        con.setCharset(system.getCharset());

    }

    public Map<String, UserConfig> getUsers() {
        return users;
    }

    public Map<String, UserConfig> getBackupUsers() {
        return _users;
    }

    public Map<String, SchemaConfig> getSchemas() {
        return schemas;
    }

    public Map<String, SchemaConfig> getBackupSchemas() {
        return _schemas;
    }

    public Map<String, PhysicalDBNode> getDataNodes() {
        return dataNodes;
    }

    public void setDataNodes(Map<String, PhysicalDBNode> map) {
        this.dataNodes = map;
    }

    public String[] getDataNodeSchemasOfDataHost(String dataHost) {
        ArrayList<String> schemas = new ArrayList<String>(30);
        for (PhysicalDBNode dn : dataNodes.values()) {
            if (dn.getDbPool().getHostName().equals(dataHost)) {
                schemas.add(dn.getDatabase());
            }
        }
        return schemas.toArray(new String[schemas.size()]);
    }

    public Map<String, PhysicalDBNode> getBackupDataNodes() {
        return _dataNodes;
    }

    public Map<String, PhysicalDBPool> getDataHosts() {
        return dataHosts;
    }

    public Map<String, PhysicalDBPool> getBackupDataHosts() {
        return _dataHosts;
    }

    public SqlEngineCluster getCluster() {
        return cluster;
    }

    public SqlEngineCluster getBackupCluster() {
        return _cluster;
    }

    public FirewallConfig getFirewall() {
        return firewall;
    }

    public FirewallConfig getBackupFirewall() {
        return _firewall;
    }

    public ReentrantLock getLock() {
        return lock;
    }

    public long getReloadTime() {
        return reloadTime;
    }

    public long getRollbackTime() {
        return rollbackTime;
    }

    public void reload(
            Map<String, UserConfig> newUsers,
            Map<String, SchemaConfig> newSchemas,
            Map<String, PhysicalDBNode> newDataNodes,
            Map<String, PhysicalDBPool> newDataHosts,
            SqlEngineCluster newCluster,
            FirewallConfig newFirewall,
            boolean reloadAll) {

        apply(newUsers, newSchemas, newDataNodes, newDataHosts, newCluster, newFirewall, reloadAll);
        this.reloadTime = TimeUtil.currentTimeMillis();
        this.status = reloadAll ? RELOAD_ALL : RELOAD;
    }

    public boolean canRollback() {
        if (_users == null || _schemas == null || _dataNodes == null
                || _dataHosts == null || _cluster == null
                || _firewall == null || status == ROLLBACK) {
            return false;
        } else {
            return true;
        }
    }

    public void rollback(
            Map<String, UserConfig> users,
            Map<String, SchemaConfig> schemas,
            Map<String, PhysicalDBNode> dataNodes,
            Map<String, PhysicalDBPool> dataHosts,
            SqlEngineCluster cluster,
            FirewallConfig firewall) {

        apply(users, schemas, dataNodes, dataHosts, cluster, firewall, status == RELOAD_ALL);
        this.rollbackTime = TimeUtil.currentTimeMillis();
        this.status = ROLLBACK;
    }

    private void apply(Map<String, UserConfig> newUsers,
                       Map<String, SchemaConfig> newSchemas,
                       Map<String, PhysicalDBNode> newDataNodes,
                       Map<String, PhysicalDBPool> newDataHosts,
                       SqlEngineCluster newCluster,
                       FirewallConfig newFirewall,
                       boolean isLoadAll) {

        final ReentrantLock lock = this.lock;
        lock.lock();
        try {

            // old 处理
            // 1、停止老的数据源心跳
            // 2、备份老的数据源配置
            //--------------------------------------------
            if (isLoadAll) {
                Map<String, PhysicalDBPool> oldDataHosts = this.dataHosts;
                if (oldDataHosts != null) {
                    for (PhysicalDBPool oldDbPool : oldDataHosts.values()) {
                        if (oldDbPool != null) {
                            oldDbPool.stopHeartbeat();
                        }
                    }
                }
                this._dataNodes = this.dataNodes;
                this._dataHosts = this.dataHosts;
            }

            this._users = this.users;
            this._schemas = this.schemas;
            this._cluster = this.cluster;
            this._firewall = this.firewall;

            // new 处理
            // 1、启动新的数据源心跳
            // 2、执行新的配置
            //---------------------------------------------------
            if (isLoadAll) {
                if (newDataHosts != null) {
                    for (PhysicalDBPool newDbPool : newDataHosts.values()) {
                        if (newDbPool != null) {
                            newDbPool.startHeartbeat();
                        }
                    }
                }
                this.dataNodes = newDataNodes;
                this.dataHosts = newDataHosts;
            }
            this.users = newUsers;
            this.schemas = newSchemas;
            this.cluster = newCluster;
            this.firewall = newFirewall;

        } finally {
            lock.unlock();
        }
    }
}