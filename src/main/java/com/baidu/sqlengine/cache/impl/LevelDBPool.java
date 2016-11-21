package com.baidu.sqlengine.cache.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.iq80.leveldb.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.cache.CachePool;
import com.baidu.sqlengine.cache.CacheStatic;

public class LevelDBPool implements CachePool {
    private static final Logger LOGGER = LoggerFactory.getLogger(LevelDBPool.class);
    private final DB cache;
    private final CacheStatic cacheStati = new CacheStatic();
    private final String name;
    private final long maxSize;

    public LevelDBPool(String name, DB db, long maxSize) {
        this.cache = db;
        this.name = name;
        this.maxSize = maxSize;
        cacheStati.setMaxSize(maxSize);
    }

    @Override
    public void putIfAbsent(Object key, Object value) {

        cache.put(toByteArray(key), toByteArray(value));
        cacheStati.incPutTimes();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(name + " add leveldb cache ,key:" + key + " value:" + value);
        }
    }

    @Override
    public Object get(Object key) {

        Object ob = toObject(cache.get(toByteArray(key)));
        if (ob != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(name + " hit cache ,key:" + key);
            }
            cacheStati.incHitTimes();
            return ob;
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(name + "  miss cache ,key:" + key);
            }
            cacheStati.incAccessTimes();
            return null;
        }
    }

    @Override
    public void clearCache() {
        LOGGER.info("clear cache " + name);
        //cache.delete(key);
        cacheStati.reset();
        //cacheStati.setMemorySize(cache.g);

    }

    @Override
    public CacheStatic getCacheStatic() {
        cacheStati.setItemSize(cacheStati.getPutTimes());
        return cacheStati;
    }

    @Override
    public long getMaxSize() {

        return maxSize;
    }

    public byte[] toByteArray(Object obj) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            LOGGER.error("toByteArrayError", ex);
        }
        return bytes;
    }

    public Object toObject(byte[] bytes) {
        Object obj = null;
        if ((bytes == null) || (bytes.length <= 0)) {
            return obj;
        }
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (IOException ex) {
            LOGGER.error("toObjectError", ex);
        } catch (ClassNotFoundException ex) {
            LOGGER.error("toObjectError", ex);
        }
        return obj;
    }

}
