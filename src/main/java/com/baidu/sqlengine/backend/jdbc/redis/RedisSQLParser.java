package com.baidu.sqlengine.backend.jdbc.redis;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import redis.clients.jedis.Jedis;

public class RedisSQLParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSQLParser.class);
    private final Jedis db;
    private final SQLStatement statement;
    private List params;
    private int pos;

    public RedisSQLParser(Jedis db, String sql) throws RedisSQLException {
        this.db = db;
        this.statement = parser(sql);
    }

    public SQLStatement parser(String s) throws RedisSQLException {
        s = s.trim();
        try {
            MySqlStatementParser parser = new MySqlStatementParser(s);
            return parser.parseStatement();
        } catch (Exception e) {
            LOGGER.error("RedisSQLParser.parserError", e);
        }
        throw new RedisSQLException.ErrorSQL(s);
    }

    public void setParams(List params) {
        this.pos = 1;
        this.params = params;
    }

    public RedisData query() throws RedisSQLException {
        if (!(statement instanceof SQLSelectStatement)) {
            //return null;
            throw new IllegalArgumentException("not a query sql statement");
        }
        RedisData redisData = new RedisData();

        SQLSelectStatement selectStmt = (SQLSelectStatement) statement;
        SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
        if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
            MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock) selectStmt.getSelect().getQuery();

            //显示的字段
            for (SQLSelectItem item : mysqlSelectQuery.getSelectList()) {
                if (!(item.getExpr() instanceof SQLAllColumnExpr)) {
                    if (item.getExpr() instanceof SQLAggregateExpr) {
                    } else {
                        redisData.setKeyColumnName(getFieldName(item));
                    }
                }

            }

            //表名
            SQLTableSource table = mysqlSelectQuery.getFrom();
            redisData.setTable(table.toString());

            SQLExpr aexpr = mysqlSelectQuery.getWhere();

            String column = "";
            Object value = null;

            if (aexpr instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr expr = (SQLBinaryOpExpr) aexpr;
                SQLExpr exprL = expr.getLeft();
                SQLExpr exprR = expr.getRight();

                if (expr.getOperator().getName().equals("=")) {
                    column = exprL.toString();
                    value = getExpValue(expr.getRight());
                }

                SQLSelectGroupByClause groupby = mysqlSelectQuery.getGroupBy();

                int limitoff = 0;
                int limitnum = 0;
                if (mysqlSelectQuery.getLimit() != null) {
                    limitoff = getSQLExprToInt(mysqlSelectQuery.getLimit().getOffset());
                    limitnum = getSQLExprToInt(mysqlSelectQuery.getLimit().getRowCount());
                }

                String result = this.db.get(value.toString());
                //            this.db.hmget();

                List<Object> resultList = new LinkedList<>();
                resultList.add(result);
                RedisData.RedisCursor cursor = new RedisData.RedisCursor(resultList);
                redisData.setCursor(cursor);
            }
        }
        return redisData;

    }

    public int executeUpdate() throws RedisSQLException {
        if (statement instanceof SQLInsertStatement) {
            return InsertData((SQLInsertStatement) statement);
        }
        if (statement instanceof SQLUpdateStatement) {
            return UpData((SQLUpdateStatement) statement);
        }
        if (statement instanceof SQLDropTableStatement) {
            return dropTable((SQLDropTableStatement) statement);
        }
        if (statement instanceof SQLDeleteStatement) {
            return DeleteDate((SQLDeleteStatement) statement);
        }
        if (statement instanceof SQLCreateTableStatement) {
            return 1;
        }
        return 1;

    }

    private int InsertData(SQLInsertStatement state) {
        return 1;
    }

    private int UpData(SQLUpdateStatement state) {
        return 1;
    }

    private int DeleteDate(SQLDeleteStatement state) {
        return 1;
    }

    private int dropTable(SQLDropTableStatement state) {
        return 1;

    }

    private int getSQLExprToInt(SQLExpr expr) {
        if (expr instanceof SQLIntegerExpr) {
            return ((SQLIntegerExpr) expr).getNumber().intValue();
        }
        return 0;
    }

    private int getSQLExprToAsc(SQLOrderingSpecification ASC) {
        if (ASC == null) {
            return 1;
        }
        if (ASC == SQLOrderingSpecification.DESC) {
            return -1;
        } else {
            return 1;
        }
    }

    public String remove(String resource, char ch) {
        StringBuffer buffer = new StringBuffer();
        int position = 0;
        char currentChar;

        while (position < resource.length()) {
            currentChar = resource.charAt(position++);
            if (currentChar != ch) {
                buffer.append(currentChar);
            }
        }
        return buffer.toString();
    }

    private Object getExpValue(SQLExpr expr) {
        if (expr instanceof SQLIntegerExpr) {
            return ((SQLIntegerExpr) expr).getNumber().intValue();
        }
        if (expr instanceof SQLNumberExpr) {
            return ((SQLNumberExpr) expr).getNumber().doubleValue();
        }
        if (expr instanceof SQLCharExpr) {
            String va = ((SQLCharExpr) expr).toString();
            return remove(va, '\'');
        }
        if (expr instanceof SQLBooleanExpr) {
            return ((SQLBooleanExpr) expr).getValue();
        }
        if (expr instanceof SQLNullExpr) {
            return null;
        }
        if (expr instanceof SQLVariantRefExpr) {
            return this.params.get(this.pos++);
        }
        return expr;

    }

    private String getExprFieldName(SQLAggregateExpr expr) {
        String field = "";
        for (SQLExpr item : expr.getArguments()) {
            field += item.toString();
        }
        return expr.getMethodName() + "(" + field + ")";

    }

    private String getFieldName2(SQLExpr item) {
        return item.toString();
    }

    private String getFieldName(SQLSelectItem item) {
        return item.toString();
    }

    private DBObject parserWhere(SQLExpr expr) {
        BasicDBObject o = new BasicDBObject();
        parserWhere(expr, o);
        return o;
    }

    @SuppressWarnings("unused")
    private void opSQLExpr(SQLBinaryOpExpr expr, BasicDBObject o) {
        SQLExpr exprL = expr.getLeft();
        if (!(exprL instanceof SQLBinaryOpExpr)) {
            if (expr.getOperator().getName().equals("=")) {
                o.put(exprL.toString(), getExpValue(expr.getRight()));
            } else {
                //BasicDBObject xo = new BasicDBObject();
                String op = "";
                if (expr.getOperator().getName().equals("<")) {
                    op = "$lt";
                }
                if (expr.getOperator().getName().equals("<=")) {
                    op = "$lte";
                }
                if (expr.getOperator().getName().equals(">")) {
                    op = "$gt";
                }
                if (expr.getOperator().getName().equals(">=")) {
                    op = "$gte";
                }

                if (expr.getOperator().getName().equals("!=")) {
                    op = "$ne";
                }
                if (expr.getOperator().getName().equals("<>")) {
                    op = "$ne";
                }
            }
        }
    }

    private void parserWhere(SQLExpr aexpr, BasicDBObject o) {
        if (aexpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr expr = (SQLBinaryOpExpr) aexpr;
            SQLExpr exprL = expr.getLeft();
            if (!(exprL instanceof SQLBinaryOpExpr)) {
                //opSQLExpr((SQLBinaryOpExpr)aexpr,o);
                if (expr.getOperator().getName().equals("=")) {
                    o.put(exprL.toString(), getExpValue(expr.getRight()));
                } else {
                    String op = "";
                    if (expr.getOperator().getName().equals("<")) {
                        op = "$lt";
                    }
                    if (expr.getOperator().getName().equals("<=")) {
                        op = "$lte";
                    }
                    if (expr.getOperator().getName().equals(">")) {
                        op = "$gt";
                    }
                    if (expr.getOperator().getName().equals(">=")) {
                        op = "$gte";
                    }

                    if (expr.getOperator().getName().equals("!=")) {
                        op = "$ne";
                    }
                    if (expr.getOperator().getName().equals("<>")) {
                        op = "$ne";
                    }
                }

            } else {
                if (expr.getOperator().getName().equals("AND")) {
                    parserWhere(exprL, o);
                    parserWhere(expr.getRight(), o);
                } else if (expr.getOperator().getName().equals("OR")) {
                    orWhere(exprL, expr.getRight(), o);
                } else {
                    throw new RuntimeException("Can't identify the operation of  of where");
                }
            }
        }

    }

    private void orWhere(SQLExpr exprL, SQLExpr exprR, BasicDBObject ob) {
        BasicDBObject xo = new BasicDBObject();
        BasicDBObject yo = new BasicDBObject();
        parserWhere(exprL, xo);
        parserWhere(exprR, yo);
        ob.put("$or", new Object[] {xo, yo});
    }
}
