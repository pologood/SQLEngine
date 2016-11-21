package com.baidu.sqlengine.parser.druid;

import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.Token;

public class SqlEngineExprParser extends MySqlExprParser {
    public static final String[] max_agg_functions =
            {"AVG", "COUNT", "GROUP_CONCAT", "MAX", "MIN", "STDDEV", "SUM", "ROW_NUMBER"};

    public SqlEngineExprParser(Lexer lexer) {
        super(lexer);
        super.aggregateFunctions = max_agg_functions;
    }

    public SqlEngineExprParser(String sql) {
        super(new SqlEngineLexer(sql));
        lexer.nextToken();
        super.aggregateFunctions = max_agg_functions;
    }

    @Override
    public SQLSelectItem parseSelectItem() {
        parseTop();
        return super.parseSelectItem();
    }

    public void parseTop() {
        if (lexer.token() == Token.TOP) {
            lexer.nextToken();

            boolean paren = false;
            if (lexer.token() == Token.LPAREN) {
                paren = true;
                lexer.nextToken();
            }

            if (paren) {
                accept(Token.RPAREN);
            }

            if (lexer.token() == Token.LITERAL_INT) {
                lexer.mark();
                lexer.nextToken();
            }
            if (lexer.token() == Token.IDENTIFIER) {
                lexer.nextToken();

            }
            if (lexer.token() == Token.EQ || lexer.token() == Token.DOT) {
                lexer.nextToken();
            } else if (lexer.token() != Token.STAR) {
                lexer.reset();
            }
            if (lexer.token() == Token.PERCENT) {
                lexer.nextToken();
            }
        }
    }
}
