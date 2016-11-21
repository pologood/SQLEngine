package com.baidu.sqlengine.parser.druid.impl;

import java.sql.SQLNonTransientException;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.route.RouteResultSet;
import com.baidu.sqlengine.util.StringUtil;

public class DruidDeleteParser extends DefaultDruidParser {
    @Override
    public void statementParse(SchemaConfig schema, RouteResultSet rrs, SQLStatement stmt)
            throws SQLNonTransientException {
        MySqlDeleteStatement delete = (MySqlDeleteStatement) stmt;
        String tableName = StringUtil.removeBackquote(delete.getTableName().getSimpleName().toUpperCase());
        ctx.addTable(tableName);
    }
}

