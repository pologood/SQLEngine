package com.baidu.sqlengine.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.config.model.SqlEngineNodeConfig;

public class SqlEngineNode {
	private static final Logger LOGGER = LoggerFactory.getLogger(SqlEngineNode.class);

	private final String name;
	private final SqlEngineNodeConfig config;

	public SqlEngineNode(SqlEngineNodeConfig config) {
		this.name = config.getName();
		this.config = config;
	}

	public String getName() {
		return name;
	}

	public SqlEngineNodeConfig getConfig() {
		return config;
	}

	public boolean isOnline() {
		return (true);
	}

}