package com.baidu.sqlengine.constant;

/**
 * 报警关键词定义
 */
public interface Alarms {
    /**
     * 默认报警关键词
     **/
    String DEFAULT = "#!SqlEngine#";

    /**
     * 集群无有效的节点可提供服务
     **/
    String CLUSTER_EMPTY = "#!CLUSTER_EMPTY#";

    /**
     * 数据节点的数据源发生切换
     **/
    String DATANODE_SWITCH = "#!DN_SWITCH#";

    /**
     * 防火墙非法用户访问
     **/
    String FIREWALL_ATTACK = "#!QT_ATTACK#";

    /**
     * 非法DML
     **/
    String DML_ATTACK = "#!DML_ATTACK#";

}
