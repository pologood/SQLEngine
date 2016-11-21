package com.baidu.sqlengine.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.backend.datasource.PhysicalDBNode;
import com.baidu.sqlengine.backend.datasource.PhysicalDBPool;
import com.baidu.sqlengine.backend.datasource.PhysicalDatasource;
import com.baidu.sqlengine.backend.jdbc.JDBCDatasource;
import com.baidu.sqlengine.backend.mysql.nio.MySQLDataSource;
import com.baidu.sqlengine.config.loader.ConfigLoader;
import com.baidu.sqlengine.config.loader.SchemaLoader;
import com.baidu.sqlengine.config.loader.xml.XMLConfigLoader;
import com.baidu.sqlengine.config.loader.xml.XMLSchemaLoader;
import com.baidu.sqlengine.config.model.DBHostConfig;
import com.baidu.sqlengine.config.model.DataHostConfig;
import com.baidu.sqlengine.config.model.DataNodeConfig;
import com.baidu.sqlengine.config.model.FirewallConfig;
import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.config.model.SystemConfig;
import com.baidu.sqlengine.config.model.UserConfig;
import com.baidu.sqlengine.config.util.ConfigException;
import com.baidu.sqlengine.route.sequence.handler.IncrSequenceTimeHandler;

public class ConfigInitializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigInitializer.class);

    private volatile SystemConfig system;
    private volatile SqlEngineCluster cluster;
    private volatile FirewallConfig firewall;
    private volatile Map<String, UserConfig> users;
    private volatile Map<String, SchemaConfig> schemas;
    private volatile Map<String, PhysicalDBNode> dataNodes;
    private volatile Map<String, PhysicalDBPool> dataHosts;

    public ConfigInitializer(boolean loadDataHost) {

        //读取rule.xml和schema.xml
        SchemaLoader schemaLoader = new XMLSchemaLoader();

        //读取server.xml
        XMLConfigLoader configLoader = new XMLConfigLoader(schemaLoader);

        schemaLoader = null;

        //加载配置
        this.system = configLoader.getSystemConfig();
        this.users = configLoader.getUserConfigs();
        this.schemas = configLoader.getSchemaConfigs();

        //是否重新加载DataHost和对应的DataNode
        if (loadDataHost) {
            this.dataHosts = initDataHosts(configLoader);
            this.dataNodes = initDataNodes(configLoader);
        }

        //权限管理
        this.firewall = configLoader.getFirewallConfig();
        this.cluster = initCobarCluster(configLoader);

        if (system.getSequnceHandlerType() == SystemConfig.SEQUENCEHANDLER_LOCAL_TIME) {
            IncrSequenceTimeHandler.getInstance().load();
        }

        /**
         * 配置文件初始化， 自检
         */
        this.selfChecking0();
    }

    private void selfChecking0() throws ConfigException {

        // 检查user与schema配置对应以及schema配置不为空
        if (users == null || users.isEmpty()) {
            throw new ConfigException("SelfCheck### user all node is empty!");

        } else {

            for (UserConfig uc : users.values()) {
                if (uc == null) {
                    throw new ConfigException("SelfCheck### users node within the item is empty!");
                }

                Set<String> authSchemas = uc.getSchemas();
                if (authSchemas == null) {
                    throw new ConfigException("SelfCheck### user " + uc.getName() + "refered schemas is empty!");
                }

                for (String schema : authSchemas) {
                    if (!schemas.containsKey(schema)) {
                        String errMsg = "SelfCheck###  schema " + schema + " refered by user " + uc.getName()
                                + " is not exist!";
                        throw new ConfigException(errMsg);
                    }
                }
            }
        }

        // schema 配置检测
        for (SchemaConfig sc : schemas.values()) {
            if (null == sc) {
                throw new ConfigException("SelfCheck### schema all node is empty!");

            } else {
                // check dataNode / dataHost 节点
                if (this.dataNodes != null && this.dataHosts != null) {
                    Set<String> dataNodeNames = sc.getAllDataNodes();
                    for (String dataNodeName : dataNodeNames) {

                        PhysicalDBNode node = this.dataNodes.get(dataNodeName);
                        if (node == null) {
                            throw new ConfigException("SelfCheck### schema dbnode is empty!");
                        }
                    }
                }
            }
        }

    }

    public void testConnection() {

        // 实际链路的连接测试
        if (this.dataNodes != null && this.dataHosts != null) {

            Map<String, Boolean> map = new HashMap<String, Boolean>();

            for (PhysicalDBNode dataNode : dataNodes.values()) {

                String database = dataNode.getDatabase();
                PhysicalDBPool pool = dataNode.getDbPool();

                for (PhysicalDatasource ds : pool.getAllDataSources()) {
                    String key = ds.getName() + "_" + database;
                    if (map.get(key) == null) {
                        map.put(key, false);

                        boolean isConnected = false;
                        try {
                            isConnected = ds.testConnection(database);
                            map.put(key, isConnected);
                        } catch (IOException e) {
                            LOGGER.warn("test conn error:", e);
                        }
                    }
                }
            }

            //
            boolean isConnectivity = true;
            for (Map.Entry<String, Boolean> entry : map.entrySet()) {
                String key = entry.getKey();
                Boolean value = entry.getValue();
                if (!value && isConnectivity) {
                    LOGGER.warn("SelfCheck### test " + key + " database connection failed ");
                    isConnectivity = false;

                } else {
                    LOGGER.info("SelfCheck### test " + key + " database connection success ");
                }
            }

            if (!isConnectivity) {
                throw new ConfigException("SelfCheck### there are some datasource connection failed, pls check!");
            }

        }

    }

    public SystemConfig getSystem() {
        return system;
    }

    public SqlEngineCluster getCluster() {
        return cluster;
    }

    public FirewallConfig getFirewall() {
        return firewall;
    }

    public Map<String, UserConfig> getUsers() {
        return users;
    }

    public Map<String, SchemaConfig> getSchemas() {
        return schemas;
    }

    public Map<String, PhysicalDBNode> getDataNodes() {
        return dataNodes;
    }

    public Map<String, PhysicalDBPool> getDataHosts() {
        return this.dataHosts;
    }

    private SqlEngineCluster initCobarCluster(ConfigLoader configLoader) {
        return new SqlEngineCluster(configLoader.getClusterConfig());
    }

    private Map<String, PhysicalDBPool> initDataHosts(ConfigLoader configLoader) {
        Map<String, DataHostConfig> nodeConfs = configLoader.getDataHosts();
        //根据DataHost建立PhysicalDBPool，其实就是实际数据库连接池，每个DataHost对应一个PhysicalDBPool
        Map<String, PhysicalDBPool> nodes = new HashMap<String, PhysicalDBPool>(
                nodeConfs.size());
        for (DataHostConfig conf : nodeConfs.values()) {
            //建立PhysicalDBPool
            PhysicalDBPool pool = getPhysicalDBPool(conf, configLoader);
            nodes.put(pool.getHostName(), pool);
        }
        return nodes;
    }

    private PhysicalDatasource[] createDataSource(DataHostConfig conf,
                                                  String hostName, String dbType, String dbDriver,
                                                  DBHostConfig[] nodes, boolean isRead) {
        PhysicalDatasource[] dataSources = new PhysicalDatasource[nodes.length];
        if (dbType.equals("mysql") && dbDriver.equals("native")) {
            for (int i = 0; i < nodes.length; i++) {
                //设置最大idle时间，默认为30分钟
                nodes[i].setIdleTimeout(system.getIdleTimeout());
                MySQLDataSource ds = new MySQLDataSource(nodes[i], conf, isRead);
                dataSources[i] = ds;
            }

        } else if (dbDriver.equals("jdbc")) {
            for (int i = 0; i < nodes.length; i++) {
                nodes[i].setIdleTimeout(system.getIdleTimeout());
                JDBCDatasource ds = new JDBCDatasource(nodes[i], conf, isRead);
                dataSources[i] = ds;
            }
        } else {
            throw new ConfigException("not supported yet !" + hostName);
        }
        return dataSources;
    }

    private PhysicalDBPool getPhysicalDBPool(DataHostConfig conf,
                                             ConfigLoader configLoader) {
        String name = conf.getName();
        //数据库类型，我们这里只讨论MySQL
        String dbType = conf.getDbType();
        //连接数据库驱动，我们这里只讨论SqlEngine自己实现的native
        String dbDriver = conf.getDbDriver();
        //针对所有写节点创建PhysicalDatasource
        PhysicalDatasource[] writeSources = createDataSource(conf, name,
                dbType, dbDriver, conf.getWriteHosts(), false);
        Map<Integer, DBHostConfig[]> readHostsMap = conf.getReadHosts();
        Map<Integer, PhysicalDatasource[]> readSourcesMap = new HashMap<Integer, PhysicalDatasource[]>(
                readHostsMap.size());
        //对于每个读节点建立key为writeHost下标value为readHost的PhysicalDatasource[]的哈希表
        for (Map.Entry<Integer, DBHostConfig[]> entry : readHostsMap.entrySet()) {
            PhysicalDatasource[] readSources = createDataSource(conf, name,
                    dbType, dbDriver, entry.getValue(), true);
            readSourcesMap.put(entry.getKey(), readSources);
        }
        PhysicalDBPool pool = new PhysicalDBPool(conf.getName(), conf,
                writeSources, readSourcesMap, conf.getBalance(),
                conf.getWriteType());
        return pool;
    }

    private Map<String, PhysicalDBNode> initDataNodes(ConfigLoader configLoader) {
        Map<String, DataNodeConfig> nodeConfs = configLoader.getDataNodes();
        Map<String, PhysicalDBNode> nodes = new HashMap<String, PhysicalDBNode>(
                nodeConfs.size());
        for (DataNodeConfig conf : nodeConfs.values()) {
            PhysicalDBPool pool = this.dataHosts.get(conf.getDataHost());
            if (pool == null) {
                throw new ConfigException("dataHost not exists "
                        + conf.getDataHost());

            }
            PhysicalDBNode dataNode = new PhysicalDBNode(conf.getName(),
                    conf.getDatabase(), pool);
            nodes.put(dataNode.getName(), dataNode);
        }
        return nodes;
    }

}