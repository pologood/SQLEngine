package com.baidu.sqlengine.parser.druid.impl;

import java.sql.SQLNonTransientException;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.route.RouteResultSet;
import com.baidu.sqlengine.parser.druid.SqlEngineSchemaStatVisitor;
import com.baidu.sqlengine.util.StringUtil;

public class DruidCreateTableParser extends DefaultDruidParser {

    @Override
    public void visitorParse(RouteResultSet rrs, SQLStatement stmt, SqlEngineSchemaStatVisitor visitor) {
    }

    @Override
    public void statementParse(SchemaConfig schema, RouteResultSet rrs, SQLStatement stmt)
            throws SQLNonTransientException {
        MySqlCreateTableStatement createStmt = (MySqlCreateTableStatement) stmt;
        if (createStmt.getQuery() != null) {
            String msg = "create table from other table not supported :" + stmt;
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
        }
        String tableName = StringUtil.removeBackquote(createStmt.getTableSource().toString().toUpperCase());
        ctx.addTable(tableName);
    }
}
