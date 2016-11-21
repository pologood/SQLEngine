package com.baidu.sqlengine.server.util;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.parser.druid.SqlEngineSchemaStatVisitor;
import com.baidu.sqlengine.server.parser.ServerParse;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SchemaUtil {
    public static SchemaInfo parseSchema(String sql) {
        SQLStatementParser parser = new MySqlStatementParser(sql);
        return parseTables(parser.parseStatement(), new SqlEngineSchemaStatVisitor());
    }

    public static String detectDefaultDb(String sql, int type) {
        String db = null;
        Map<String, SchemaConfig> schemaConfigMap = SqlEngineServer.getInstance().getConfig()
                .getSchemas();
        if (ServerParse.SELECT == type) {
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
            if ((schemaInfo == null || schemaInfo.table == null) && !schemaConfigMap.isEmpty()) {
                db = schemaConfigMap.entrySet().iterator().next().getKey();
            }

            if (schemaInfo != null && schemaInfo.schema != null) {

                if (schemaConfigMap.containsKey(schemaInfo.schema)) {
                    db = schemaInfo.schema;

                    /**
                     * 对 MySQL 自带的元数据库 information_schema 进行返回
                     */
                } else if ("information_schema".equalsIgnoreCase(schemaInfo.schema)) {
                    db = "information_schema";
                }
            }
        } else if (ServerParse.INSERT == type || ServerParse.UPDATE == type || ServerParse.DELETE == type
                || ServerParse.DDL == type) {
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
            if (schemaInfo != null && schemaInfo.schema != null && schemaConfigMap.containsKey(schemaInfo.schema)) {
                db = schemaInfo.schema;
            }
        } else if ((ServerParse.SHOW == type || ServerParse.USE == type || ServerParse.EXPLAIN == type
                            || ServerParse.SET == type
                            || ServerParse.HELP == type || ServerParse.DESCRIBE == type)
                && !schemaConfigMap.isEmpty()) {
            //兼容mysql gui  不填默认database
            db = schemaConfigMap.entrySet().iterator().next().getKey();
        }
        return db;
    }

    public static String parseShowTableSchema(String sql) {
        Matcher ma = pattern.matcher(sql);
        if (ma.matches() && ma.groupCount() >= 5) {
            return ma.group(5);
        }
        return null;
    }

    private static SchemaInfo parseTables(SQLStatement stmt, SchemaStatVisitor schemaStatVisitor) {

        stmt.accept(schemaStatVisitor);
        String key = schemaStatVisitor.getCurrentTable();
        if (key != null && key.contains("`")) {
            key = key.replaceAll("`", "");
        }

        if (key != null) {
            SchemaInfo schemaInfo = new SchemaInfo();
            int pos = key.indexOf(".");
            if (pos > 0) {
                schemaInfo.schema = key.substring(0, pos);
                schemaInfo.table = key.substring(pos + 1);
            } else {
                schemaInfo.table = key;
            }
            return schemaInfo;
        }

        return null;
    }

    public static class SchemaInfo {
        public String table;
        public String schema;

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("SchemaInfo{");
            sb.append("table='").append(table).append('\'');
            sb.append(", schema='").append(schema).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

    private static Pattern pattern =
            Pattern.compile("^\\s*(SHOW)\\s+(FULL)*\\s*(TABLES)\\s+(FROM)\\s+([a-zA-Z_0-9]+)\\s*([a-zA-Z_0-9\\s]*)",
                    Pattern.CASE_INSENSITIVE);

}
