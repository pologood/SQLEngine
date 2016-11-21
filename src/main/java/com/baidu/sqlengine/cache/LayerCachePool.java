package com.baidu.sqlengine.cache;

import java.util.Map;

/**
 * Layered cache pool
 */
public interface LayerCachePool extends CachePool {

    void putIfAbsent(String primaryKey, Object secondKey, Object value);

    Object get(String primaryKey, Object secondKey);

    Map<String, CacheStatic> getAllCacheStatic();
}