package com.baidu.sqlengine.backend.jdbc.mongodb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

public class MongoStatement implements Statement {
    private MongoConnection conn;
    private final int type;
    private final int concurrency;
    private final int holdability;
    private int fetchSize = 0;
    private MongoResultSet last;

    public MongoStatement(MongoConnection conn, int type, int concurrency, int holdability) {
        this.conn = conn;
        this.type = type;
        this.concurrency = concurrency;
        this.holdability = holdability;

        if (this.type != 0) {
            throw new UnsupportedOperationException("type not supported yet");
        }
        if (this.concurrency != 0) {
            throw new UnsupportedOperationException("concurrency not supported yet");
        }
        if (this.holdability != 0) {
            throw new UnsupportedOperationException("holdability not supported yet");
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();//return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        MongoData mongo = new MongoSQLParser(this.conn.getDB(), sql).query();
        if ((this.fetchSize > 0)
                && (mongo.getCursor() != null)) {
            //设置每次网络请求的最大记录数
            mongo.getCursor().batchSize(this.fetchSize);
        }
        this.last = new MongoResultSet(mongo, this.conn.getSchema());
        return this.last;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        // 执行更新语句
        return new MongoSQLParser(this.conn.getDB(), sql).executeUpdate();
    }

    @Override
    public void close() throws SQLException {
        this.conn = null;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        // 获取可以为此 Statement 对象所生成 ResultSet 对象中的字符和二进制列值返回的最大字节数。
        return 0;//this.fetchSize;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        //this.fetchSize=max;
    }

    @Override
    public int getMaxRows() throws SQLException {
        // 获取由此 Statement 对象生成的 ResultSet 对象可以包含的最大行数。
        return 0;//this._maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        //this._maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        // 将转义处理设置为开或关。
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        // Statement 对象执行的秒数设置，超时设置。
    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {
        // 将 SQL 指针名称设置为给定的 String，后续 Statement 对象的 execute 方法将使用此字符串。
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return this.last;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        // 记录变更的数量，ResultSet 对象或没有更多结果，则返回 -1
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        // 移动到此 Statement 对象的下一个结果，如果其为 ResultSet 对象，则返回 true，并隐式关闭利用方法 getResultSet 获取的所有当前 ResultSet 对象。
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        // 此 Statement 对象创建的 ResultSet 对象中将按该方向处理行。
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // 获取结果集合的行数，该数是根据此 Statement 对象生成的 ResultSet 对象的默认获取大小。
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        // 获取结果集合的行数，该数是根据此 Statement 对象生成的 ResultSet 对象的默认获取大小。
        return this.fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        // 对象生成的 ResultSet 对象的结果集合并发性
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        // 对象生成的 ResultSet 对象的结果集合类型。
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        // 新增批处理
        throw new UnsupportedOperationException("batch not supported");
    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        // 将一批命令提交给数据库来执行，如果全部命令执行成功，则返回更新计数组成的数组。
        return null;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.conn;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        // 将此 Statement 对象移动到下一个结果，根据给定标志指定的指令处理所有当前 ResultSet 对象；如果下一个结果为 ResultSet 对象，则返回 true。
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        // 获取由于执行此 Statement 对象而创建的所有自动生成的键。
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.conn == null;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        // 请求将 Statement 池化或非池化
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

}
