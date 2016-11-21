package com.baidu.sqlengine.config.loader;

import java.util.Map;

import com.baidu.sqlengine.config.model.ClusterConfig;
import com.baidu.sqlengine.config.model.DataHostConfig;
import com.baidu.sqlengine.config.model.DataNodeConfig;
import com.baidu.sqlengine.config.model.FirewallConfig;
import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.config.model.SystemConfig;
import com.baidu.sqlengine.config.model.UserConfig;

public interface ConfigLoader {

    SchemaConfig getSchemaConfig(String schema);

    Map<String, SchemaConfig> getSchemaConfigs();

    Map<String, DataNodeConfig> getDataNodes();

    Map<String, DataHostConfig> getDataHosts();

    SystemConfig getSystemConfig();

    UserConfig getUserConfig(String user);

    Map<String, UserConfig> getUserConfigs();

    FirewallConfig getFirewallConfig();

    ClusterConfig getClusterConfig();
}