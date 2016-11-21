package com.baidu.sqlengine.parser.druid;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.util.JdbcConstants;

public class SqlEngineStatementParser extends MySqlStatementParser {
    private static final String LOW_PRIORITY = "LOW_PRIORITY";
    private static final String LOCAL = "LOCAL";
    private static final String IGNORE = "IGNORE";
    private static final String CHARACTER = "CHARACTER";

    public SqlEngineStatementParser(String sql) {
        super(sql);
        selectExprParser = new SqlEngineExprParser(sql);
    }

    public SqlEngineStatementParser(Lexer lexer) {
        super(lexer);
        selectExprParser = new SqlEngineExprParser(lexer);
    }

    protected SQLExprParser selectExprParser;

    @Override
    public SQLSelectStatement parseSelect() {

        SqlEngineSelectParser selectParser = new SqlEngineSelectParser(this.selectExprParser);
        return new SQLSelectStatement(selectParser.select(), JdbcConstants.MYSQL);
    }

}
