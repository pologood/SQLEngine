package com.baidu.sqlengine.backend.jdbc.redis;

import java.sql.SQLException;

@SuppressWarnings("serial")
public class RedisSQLException extends SQLException
{

	public RedisSQLException(String msg)
    {
        super(msg);
    }

    public static class ErrorSQL extends RedisSQLException {

		ErrorSQL(String sql)
        {
            super(sql);
        }
    }
}
