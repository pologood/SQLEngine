package com.baidu.sqlengine.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.baidu.sqlengine.config.model.ClusterConfig;
import com.baidu.sqlengine.config.model.SqlEngineNodeConfig;

public final class SqlEngineCluster {

    private final Map<String, SqlEngineNode> nodes;
    private final Map<String, List<String>> groups;

    public SqlEngineCluster(ClusterConfig clusterConf) {
        this.nodes = new HashMap<String, SqlEngineNode>(clusterConf.getNodes().size());
        this.groups = clusterConf.getGroups();
        for (SqlEngineNodeConfig conf : clusterConf.getNodes().values()) {
            String name = conf.getName();
            SqlEngineNode node = new SqlEngineNode(conf);
            this.nodes.put(name, node);
        }
    }

    public Map<String, SqlEngineNode> getNodes() {
        return nodes;
    }

    public Map<String, List<String>> getGroups() {
        return groups;
    }

}