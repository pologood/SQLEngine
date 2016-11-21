package com.baidu.sqlengine.cache;

/**
 * simple cache pool for implement
 */
public interface CachePool {

    void putIfAbsent(Object key, Object value);

    Object get(Object key);

    void clearCache();

    CacheStatic getCacheStatic();

    long getMaxSize();
}