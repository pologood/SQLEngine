package com.baidu.sqlengine.backend.jdbc.redis;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class RedisData {

    private RedisCursor cursor;
    private List<String> keys;
    private String keyColumnName;
    private long count;
    private String table;

    private HashMap<String, Integer> fieldMap = new HashMap<String, Integer>();
    private boolean type = false;

    public RedisData() {
        this.count = 0;
        this.cursor = null;
    }

    public static class RedisCursor {

        private boolean haveNext = true;
        private int cur;
        private List<Object> datas;

        public RedisCursor() {
            this.datas = new LinkedList<>();
            cur = 0;
        }

        public RedisCursor(List<Object> datas) {
            this.datas = datas;
            cur = 0;
        }

        public Object next() {
            Object result = datas.get(cur);
            if (++cur == datas.size()) {
                haveNext = false;
            }
            return result;
        }

        public boolean hasNext() {
            return haveNext;
        }

    }

    public String getKeyColumnName() {
        return keyColumnName;
    }

    public void setKeyColumnName(String keyColumnName) {
        this.keyColumnName = keyColumnName;
    }

    public List<String> getKeys() {
        return keys;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    public long getCount() {
        return this.count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getTable() {
        return this.table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public static int getObjectToType(Object ob) {
        if (ob instanceof Integer) {
            return Types.INTEGER;
        } else if (ob instanceof Boolean) {
            return Types.BOOLEAN;
        } else if (ob instanceof Byte) {
            return Types.BIT;
        } else if (ob instanceof Short) {
            return Types.INTEGER;
        } else if (ob instanceof Float) {
            return Types.FLOAT;
        } else if (ob instanceof Long) {
            return Types.BIGINT;
        } else if (ob instanceof Double) {
            return Types.DOUBLE;
        } else if (ob instanceof Date) {
            return Types.DATE;
        } else if (ob instanceof Time) {
            return Types.TIME;
        } else if (ob instanceof Timestamp) {
            return Types.TIMESTAMP;
        } else if (ob instanceof String) {
            return Types.VARCHAR;
        } else {
            return Types.VARCHAR;
        }
    }

    public void setField(String field, int ftype) {
        fieldMap.put(field, ftype);
    }

    public HashMap<String, Integer> getFields() {
        return this.fieldMap;
    }

    public boolean getType() {
        return this.type;
    }

    public RedisCursor getCursor() {
        return this.cursor;
    }

    public RedisCursor setCursor(RedisCursor cursor) {
        return this.cursor = cursor;
    }

}
