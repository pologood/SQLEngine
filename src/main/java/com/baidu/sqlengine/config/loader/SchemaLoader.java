package com.baidu.sqlengine.config.loader;

import java.util.Map;

import com.baidu.sqlengine.config.model.DataHostConfig;
import com.baidu.sqlengine.config.model.DataNodeConfig;
import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.config.model.rule.TableRuleConfig;

public interface SchemaLoader {

    Map<String, TableRuleConfig> getTableRules();

    Map<String, DataHostConfig> getDataHosts();

    Map<String, DataNodeConfig> getDataNodes();

    Map<String, SchemaConfig> getSchemas();

}