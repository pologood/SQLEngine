package com.baidu.sqlengine.route.factory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.config.model.SystemConfig;
import com.baidu.sqlengine.route.RouteStrategy;
import com.baidu.sqlengine.route.impl.DruidSqlEngineRouteStrategy;

/**
 * 路由策略工厂类
 */
public class RouteStrategyFactory {
    private static RouteStrategy defaultStrategy = null;
    private static volatile boolean isInit = false;
    private static ConcurrentMap<String, RouteStrategy> strategyMap = new ConcurrentHashMap<String, RouteStrategy>();

    public static void init() {
        SystemConfig config = SqlEngineServer.getInstance().getConfig().getSystem();

        String defaultSqlParser = config.getDefaultSqlParser();
        defaultSqlParser = defaultSqlParser == null ? "" : defaultSqlParser;
        //修改为ConcurrentHashMap，避免并发问题
        strategyMap.putIfAbsent("druidparser", new DruidSqlEngineRouteStrategy());

        defaultStrategy = strategyMap.get(defaultSqlParser);
        if (defaultStrategy == null) {
            defaultStrategy = strategyMap.get("druidparser");
            defaultSqlParser = "druidparser";
        }
        config.setDefaultSqlParser(defaultSqlParser);
        isInit = true;
    }

    private RouteStrategyFactory() {

    }

    public static RouteStrategy getRouteStrategy() {
        return defaultStrategy;
    }

    public static RouteStrategy getRouteStrategy(String parserType) {
        return strategyMap.get(parserType);
    }
}
