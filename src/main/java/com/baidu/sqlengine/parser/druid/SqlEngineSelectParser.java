package com.baidu.sqlengine.parser.druid;

import com.alibaba.druid.sql.dialect.mysql.parser.MySqlSelectParser;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.Token;

public class SqlEngineSelectParser extends MySqlSelectParser {
    public SqlEngineSelectParser(SQLExprParser exprParser) {
        super(exprParser);
    }

    public SqlEngineSelectParser(String sql) {
        super(sql);
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
