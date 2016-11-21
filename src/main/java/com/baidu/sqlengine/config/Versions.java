package com.baidu.sqlengine.config;

public abstract class Versions {

    /**
     * 协议版本
     **/
    public static final byte PROTOCOL_VERSION = 10;

    /**
     * 服务器版本
     **/
    public static byte[] SERVER_VERSION = "5.6.29-SqlEngine-1.0-BETA-20161102".getBytes();

    public static void setServerVersion(String version) {
        byte[] mysqlVersionPart = version.getBytes();
        int startIndex;
        for (startIndex = 0; startIndex < SERVER_VERSION.length; startIndex++) {
            if (SERVER_VERSION[startIndex] == '-') {
                break;
            }
        }

        // 重新拼接sqlEngine version字节数组
        byte[] newSqlEngineVersion = new byte[mysqlVersionPart.length + SERVER_VERSION.length - startIndex];
        System.arraycopy(mysqlVersionPart, 0, newSqlEngineVersion, 0, mysqlVersionPart.length);
        System.arraycopy(SERVER_VERSION, startIndex, newSqlEngineVersion, mysqlVersionPart.length,
                SERVER_VERSION.length - startIndex);
        SERVER_VERSION = newSqlEngineVersion;
    }
}
