package com.baidu.sqlengine.route;

import java.sql.SQLNonTransientException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.cache.CachePool;
import com.baidu.sqlengine.cache.CacheService;
import com.baidu.sqlengine.cache.LayerCachePool;
import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.config.model.SystemConfig;
import com.baidu.sqlengine.route.factory.RouteStrategyFactory;
import com.baidu.sqlengine.server.ServerConnection;
import com.baidu.sqlengine.server.parser.ServerParse;

public class RouteService {
    private static final Logger LOGGER = LoggerFactory.getLogger(RouteService.class);

    private final CachePool sqlRouteCache;
    private final LayerCachePool tableId2DataNodeCache;

    public RouteService(CacheService cachService) {
        sqlRouteCache = cachService.getCachePool("SQLRouteCache");
        tableId2DataNodeCache = (LayerCachePool) cachService.getCachePool("TableID2DataNodeCache");
    }

    public LayerCachePool getTableId2DataNodeCache() {
        return tableId2DataNodeCache;
    }

    public RouteResultSet route(SystemConfig sysconf, SchemaConfig schema,
                                int sqlType, String stmt, String charset, ServerConnection sc)
            throws SQLNonTransientException {
        RouteResultSet rrs = null;
        String cacheKey = null;

        /**
         *  SELECT 类型的SQL, 检测
         */
        if (sqlType == ServerParse.SELECT) {
            cacheKey = schema.getName() + stmt;
            rrs = (RouteResultSet) sqlRouteCache.get(cacheKey);
            if (rrs != null) {
                return rrs;
            }
        }

        stmt = stmt.trim();
        rrs = RouteStrategyFactory.getRouteStrategy().route(sysconf, schema, sqlType, stmt,
                charset, sc, tableId2DataNodeCache);

        if (rrs != null && sqlType == ServerParse.SELECT && rrs.isCacheAble()) {
            sqlRouteCache.putIfAbsent(cacheKey, rrs);
        }
        return rrs;
    }

}