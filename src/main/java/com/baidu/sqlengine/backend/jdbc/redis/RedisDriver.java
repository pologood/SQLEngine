package com.baidu.sqlengine.backend.jdbc.redis;

import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.util.StringUtil;

public class RedisDriver implements Driver {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(RedisDriver.class);
    static final String PREFIX = "redis://";
    private DriverPropertyInfoHelper propertyInfoHelper = new DriverPropertyInfoHelper();

    static {
        try {
            DriverManager.registerDriver(new RedisDriver());
        } catch (SQLException e) {
            LOGGER.error("initError", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        URI uri = null;
        if ((uri = parseURL(url, info)) == null) {
            return null;
        }

        RedisConnection result = null;
        try {
            result = new RedisConnection(uri, url);
        } catch (Exception e) {
            throw new SQLException("Unexpected exception: " + e.getMessage(), e);
        }

        return result;
    }

    private URI parseURL(String url, Properties defaults) {
        if (url == null) {
            return null;
        }

        if (!StringUtil.startsWithIgnoreCase(url, PREFIX)) {
            return null;
        }

        try {
            return new URI(url);
        } catch (Exception e) {
            LOGGER.error("parseURLError", e);
            return null;
        }

    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (StringUtil.startsWithIgnoreCase(url, PREFIX)) {
            return true;
        }
        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException {

        return propertyInfoHelper.getPropertyInfo();
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

}
