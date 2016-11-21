package com.baidu.sqlengine.parser.druid.impl;

import java.sql.SQLNonTransientException;

import com.alibaba.druid.sql.ast.SQLStatement;

import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;

import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.route.RouteResultSet;
import com.baidu.sqlengine.parser.druid.SqlEngineSchemaStatVisitor;
import com.baidu.sqlengine.util.StringUtil;

/**
 * alter table 语句解析
 */
public class DruidAlterTableParser extends DefaultDruidParser {
    @Override
    public void visitorParse(RouteResultSet rrs, SQLStatement stmt, SqlEngineSchemaStatVisitor visitor)
            throws SQLNonTransientException {

    }

    @Override
    public void statementParse(SchemaConfig schema, RouteResultSet rrs, SQLStatement stmt)
            throws SQLNonTransientException {
        SQLAlterTableStatement alterTable = (SQLAlterTableStatement) stmt;
        String tableName = StringUtil.removeBackquote(alterTable.getTableSource().toString().toUpperCase());
        ctx.addTable(tableName);
    }

}
