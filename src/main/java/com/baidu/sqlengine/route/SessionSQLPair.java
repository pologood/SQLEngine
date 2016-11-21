package com.baidu.sqlengine.route;

import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.server.NonBlockingSession;

public class SessionSQLPair {
    public final NonBlockingSession session;

    public final SchemaConfig schema;
    public final String sql;
    public final int type;

    public SessionSQLPair(NonBlockingSession session, SchemaConfig schema, String sql, int type) {
        super();
        this.session = session;
        this.schema = schema;
        this.sql = sql;
        this.type = type;
    }

}
