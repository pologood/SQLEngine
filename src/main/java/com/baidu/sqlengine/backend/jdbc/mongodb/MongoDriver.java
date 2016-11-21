package com.baidu.sqlengine.backend.jdbc.mongodb;

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
import com.mongodb.MongoClientURI;

public class MongoDriver implements Driver {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MongoDriver.class);
    static final String PREFIX = "mongodb://";
    private DriverPropertyInfoHelper propertyInfoHelper = new DriverPropertyInfoHelper();

    static {
        try {
            DriverManager.registerDriver(new MongoDriver());
        } catch (SQLException e) {
            LOGGER.error("initError", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        MongoClientURI mcu = null;
        if ((mcu = parseURL(url, info)) == null) {
            return null;
        }

        MongoConnection result = null;
        //System.out.print(info);
        try {
            result = new MongoConnection(mcu, url);
        } catch (Exception e) {
            throw new SQLException("Unexpected exception: " + e.getMessage(), e);
        }

        return result;
    }

    private MongoClientURI parseURL(String url, Properties defaults) {
        if (url == null) {
            return null;
        }

        if (!StringUtil.startsWithIgnoreCase(url, PREFIX)) {
            return null;
        }

        try {
            return new MongoClientURI(url);
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
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
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
