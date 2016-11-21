package com.baidu.sqlengine.parser.druid;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.druid.sql.dialect.mysql.parser.MySqlLexer;
import com.alibaba.druid.sql.parser.Keywords;
import com.alibaba.druid.sql.parser.Token;

public class SqlEngineLexer extends MySqlLexer {
    public final static Keywords DEFAULT_SQL_ENGINE_KEYWORDS;

    static {
        Map<String, Token> map = new HashMap<String, Token>();

        map.putAll(Keywords.DEFAULT_KEYWORDS.getKeywords());

        map.put("DUAL", Token.DUAL);
        map.put("FALSE", Token.FALSE);
        map.put("IDENTIFIED", Token.IDENTIFIED);
        map.put("IF", Token.IF);
        map.put("KILL", Token.KILL);

        map.put("LIMIT", Token.LIMIT);
        map.put("TRUE", Token.TRUE);
        map.put("BINARY", Token.BINARY);
        map.put("SHOW", Token.SHOW);
        map.put("CACHE", Token.CACHE);
        map.put("ANALYZE", Token.ANALYZE);
        map.put("OPTIMIZE", Token.OPTIMIZE);
        map.put("ROW", Token.ROW);
        map.put("BEGIN", Token.BEGIN);
        map.put("END", Token.END);

        map.put("TOP", Token.TOP);

        DEFAULT_SQL_ENGINE_KEYWORDS = new Keywords(map);
    }

    public SqlEngineLexer(char[] input, int inputLength, boolean skipComment) {
        super(input, inputLength, skipComment);
        super.keywods = DEFAULT_SQL_ENGINE_KEYWORDS;
    }

    public SqlEngineLexer(String input) {
        super(input);
        super.keywods = DEFAULT_SQL_ENGINE_KEYWORDS;
    }
}
