package com.baidu.sqlengine.parser.druid;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.parser.druid.impl.DefaultDruidParser;
import com.baidu.sqlengine.parser.druid.impl.DruidAlterTableParser;
import com.baidu.sqlengine.parser.druid.impl.DruidCreateTableParser;
import com.baidu.sqlengine.parser.druid.impl.DruidDeleteParser;
import com.baidu.sqlengine.parser.druid.impl.DruidInsertParser;
import com.baidu.sqlengine.parser.druid.impl.DruidSelectParser;
import com.baidu.sqlengine.parser.druid.impl.DruidUpdateParser;

/**
 * DruidParser的工厂类
 */
public class DruidParserFactory {

    public static DruidParser create(SchemaConfig schema, SQLStatement statement, SchemaStatVisitor visitor) {
        DruidParser parser = null;
        if (statement instanceof SQLSelectStatement) {
            parser = new DruidSelectParser();

        } else if (statement instanceof MySqlInsertStatement) {
            parser = new DruidInsertParser();
        } else if (statement instanceof MySqlDeleteStatement) {
            parser = new DruidDeleteParser();
        } else if (statement instanceof MySqlCreateTableStatement) {
            parser = new DruidCreateTableParser();
        } else if (statement instanceof MySqlUpdateStatement) {
            parser = new DruidUpdateParser();
        } else if (statement instanceof SQLAlterTableStatement) {
            parser = new DruidAlterTableParser();
        } else {
            parser = new DefaultDruidParser();
        }

        return parser;
    }

    private static List<String> parseTables(SQLStatement stmt, SchemaStatVisitor schemaStatVisitor) {
        List<String> tables = new ArrayList<>();
        stmt.accept(schemaStatVisitor);

        if (schemaStatVisitor.getAliasMap() != null) {
            for (Map.Entry<String, String> entry : schemaStatVisitor.getAliasMap().entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (value != null && value.indexOf("`") >= 0) {
                    value = value.replaceAll("`", "");
                }
                //表名前面带database的，去掉
                if (key != null) {
                    int pos = key.indexOf("`");
                    if (pos > 0) {
                        key = key.replaceAll("`", "");
                    }
                    pos = key.indexOf(".");
                    if (pos > 0) {
                        key = key.substring(pos + 1);
                    }

                    if (key.equals(value)) {
                        tables.add(key.toUpperCase());
                    }
                }
            }

        }
        return tables;
    }

}
