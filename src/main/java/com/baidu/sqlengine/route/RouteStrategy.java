package com.baidu.sqlengine.route;

import java.sql.SQLNonTransientException;

import com.baidu.sqlengine.cache.LayerCachePool;
import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.config.model.SystemConfig;
import com.baidu.sqlengine.server.ServerConnection;

/**
 * 路由策略接口
 */
public interface RouteStrategy {
    RouteResultSet route(SystemConfig sysConfig, SchemaConfig schema, int sqlType, String origSQL, String charset,
                         ServerConnection sc, LayerCachePool cachePool) throws SQLNonTransientException;
}
