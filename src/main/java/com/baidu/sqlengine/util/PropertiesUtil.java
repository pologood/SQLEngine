package com.baidu.sqlengine.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Property文件加载器
 */
public class PropertiesUtil {
    public static Properties loadProps(String propsFile) {
        Properties props = new Properties();
        InputStream inp = Thread.currentThread().getContextClassLoader().getResourceAsStream(propsFile);

        if (inp == null) {
            throw new java.lang.RuntimeException("time sequnce properties not found " + propsFile);
        }
        try {
            props.load(inp);
        } catch (IOException e) {
            throw new java.lang.RuntimeException(e);
        }
        return props;
    }
}
